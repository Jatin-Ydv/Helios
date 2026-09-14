package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/helios-commerce/helios/platform/aerospikex"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Catalog: Postgres + Kafka. Product cache is Aerospike (not Redis).
	rt, err := boot.New(ctx, "catalog", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	asClient, err := aerospikex.Connect(rt.Cfg.AerospikeHosts)
	if err != nil {
		rt.Log.Error("aerospike", "err", err)
		os.Exit(1)
	}
	rt.Health.Register("aerospike", func(ctx context.Context) error {
		return aerospikex.Ping(ctx, asClient)
	})
	cache := aerospikex.NewCache(asClient, rt.Cfg.AerospikeNamespace)
	svc := &catalog{rt: rt, cache: cache}
	rt.Server.Router.Route("/v1/products", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/", svc.create)
		r.Get("/", svc.list)
		r.Get("/{id}", svc.get)
		r.Put("/{id}", svc.update)
		r.Delete("/{id}", svc.delete)
	})
	rt.StartOutboxRelay(ctx)
	go serve(rt)
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { cancel(); return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "kafka", Fn: func(ctx context.Context) error { return rt.Pub.Close() }},
		shutdown.Hook{Name: "db", Fn: func(ctx context.Context) error { rt.Pool.Close(); return nil }},
		shutdown.Hook{Name: "aerospike", Fn: func(ctx context.Context) error { asClient.Close(); return nil }},
		shutdown.Hook{Name: "otel", Fn: rt.Obs.Shutdown},
	)
}

func serve(rt *boot.Runtime) {
	rt.Log.Info("listen", "addr", rt.Cfg.HTTPAddr)
	if err := rt.Server.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		rt.Log.Error("http", "err", err)
	}
}

type catalog struct {
	rt    *boot.Runtime
	cache *aerospikex.Cache
}

type product struct {
	ID          string            `json:"id"`
	SKU         string            `json:"sku"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Amount      int64             `json:"amount"`
	Currency    string            `json:"currency"`
	Attributes  map[string]string `json:"attributes"`
	Active      bool              `json:"active"`
	Version     int               `json:"version"`
}

func (c *catalog) create(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var p product
	if err := httpx.Decode(r, &p); err != nil || p.SKU == "" || p.Name == "" {
		httpx.Problem(w, http.StatusBadRequest, "sku and name required")
		return
	}
	if p.Currency == "" {
		p.Currency = "USD"
	}
	p.ID = id.NewULID()
	p.Active = true
	p.Version = 1
	if p.Attributes == nil {
		p.Attributes = map[string]string{}
	}
	ctx := r.Context()
	err := database.InTx(ctx, c.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		attr, _ := json.Marshal(p.Attributes)
		_, err := tx.Exec(ctx, `
			INSERT INTO products (id, tenant_id, sku, name, description, amount, currency, attributes, active)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true)`,
			p.ID, ident.TenantID, p.SKU, p.Name, p.Description, p.Amount, p.Currency, attr)
		if err != nil {
			return err
		}
		return c.emit(ctx, tx, events.ProductUpserted, p)
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "sku exists")
		return
	}
	_ = c.cache.Set(ctx, cacheKey(ident.TenantID, p.ID), p, 5*time.Minute)
	httpx.JSON(w, http.StatusCreated, p)
}

func (c *catalog) update(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	idPath := chi.URLParam(r, "id")
	var p product
	if err := httpx.Decode(r, &p); err != nil {
		httpx.Problem(w, http.StatusBadRequest, "invalid json")
		return
	}
	ctx := r.Context()
	err := database.InTx(ctx, c.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		attr, _ := json.Marshal(p.Attributes)
		err := tx.QueryRow(ctx, `
			UPDATE products SET name=$1, description=$2, amount=$3, currency=$4, attributes=$5,
			       active=$6, version=version+1, updated_at=now()
			WHERE id=$7 AND tenant_id=$8 AND version=$9
			RETURNING sku, version`,
			p.Name, p.Description, p.Amount, coalesce(p.Currency, "USD"), attr, p.Active, idPath, ident.TenantID, p.Version,
		).Scan(&p.SKU, &p.Version)
		if err != nil {
			return err
		}
		p.ID = idPath
		return c.emit(ctx, tx, events.ProductUpserted, p)
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "version conflict or missing product")
		return
	}
	_ = c.cache.Del(ctx, cacheKey(ident.TenantID, p.ID))
	httpx.JSON(w, http.StatusOK, p)
}

func (c *catalog) get(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	pid := chi.URLParam(r, "id")
	var p product
	hit, err := c.cache.Get(r.Context(), cacheKey(ident.TenantID, pid), &p)
	if err == nil && hit {
		c.rt.Metrics.CacheHit.WithLabelValues("product", "hit").Inc()
		httpx.JSON(w, http.StatusOK, p)
		return
	}
	c.rt.Metrics.CacheHit.WithLabelValues("product", "miss").Inc()
	p, err = c.load(r.Context(), ident.TenantID, pid)
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "product")
		return
	}
	_ = c.cache.Set(r.Context(), cacheKey(ident.TenantID, pid), p, 5*time.Minute)
	httpx.JSON(w, http.StatusOK, p)
}

func (c *catalog) list(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	rows, err := c.rt.Pool.Query(r.Context(), `
		SELECT id, sku, name, description, amount, currency, attributes, active, version
		FROM products WHERE tenant_id=$1 ORDER BY updated_at DESC LIMIT 100`, ident.TenantID)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "query")
		return
	}
	defer rows.Close()
	out := []product{}
	for rows.Next() {
		var p product
		var attr []byte
		if err := rows.Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Amount, &p.Currency, &attr, &p.Active, &p.Version); err != nil {
			httpx.Problem(w, http.StatusInternalServerError, "scan")
			return
		}
		_ = json.Unmarshal(attr, &p.Attributes)
		out = append(out, p)
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": out})
}

func (c *catalog) delete(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	pid := chi.URLParam(r, "id")
	ctx := r.Context()
	err := database.InTx(ctx, c.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var sku string
		if err := tx.QueryRow(ctx, `UPDATE products SET active=false, updated_at=now() WHERE id=$1 AND tenant_id=$2 RETURNING sku`, pid, ident.TenantID).Scan(&sku); err != nil {
			return err
		}
		env, err := events.New(ctx, events.ProductDeleted, "catalog", "product", pid, events.ProductPayload{ProductID: pid, SKU: sku, Active: false})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "product")
		return
	}
	_ = c.cache.Del(ctx, cacheKey(ident.TenantID, pid))
	w.WriteHeader(http.StatusNoContent)
}

func (c *catalog) load(ctx context.Context, tid, pid string) (product, error) {
	var p product
	var attr []byte
	err := c.rt.Pool.QueryRow(ctx, `
		SELECT id, sku, name, description, amount, currency, attributes, active, version
		FROM products WHERE id=$1 AND tenant_id=$2`, pid, tid).
		Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Amount, &p.Currency, &attr, &p.Active, &p.Version)
	if err != nil {
		return p, err
	}
	_ = json.Unmarshal(attr, &p.Attributes)
	return p, nil
}

func (c *catalog) emit(ctx context.Context, tx pgx.Tx, typ string, p product) error {
	env, err := events.New(ctx, typ, "catalog", "product", p.ID, events.ProductPayload{
		ProductID: p.ID, SKU: p.SKU, Name: p.Name, Description: p.Description,
		Amount: p.Amount, Currency: p.Currency, Attributes: p.Attributes, Active: p.Active,
	})
	if err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, env)
}

// Aerospike user key: tenant:product (set=products, bin=json).
func cacheKey(tid, pid string) string { return tid + ":" + pid }
func coalesce(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
