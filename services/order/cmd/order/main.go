package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/idempotency"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "order", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc := &orders{rt: rt}
	rt.Server.Router.Route("/v1/orders", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/", svc.create)
		r.Get("/{id}", svc.get)
		r.Post("/{id}/confirm", svc.transition("confirmed", events.OrderConfirmed))
		r.Post("/{id}/cancel", svc.transition("cancelled", events.OrderCancelled))
		r.Post("/{id}/fail", svc.transition("failed", events.OrderFailed))
	})
	rt.StartOutboxRelay(ctx)
	gs := grpcx.NewServer(rt.InternalToken())
	heliosv1.RegisterOrderServiceServer(gs, &orderGRPC{svc: svc})
	go func() {
		if err := grpcx.ListenAndServe(rt.Log, rt.Cfg.GRPCAddr, gs); err != nil {
			rt.Log.Error("grpc", "err", err)
		}
	}()
	go serve(rt)
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { cancel(); return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "grpc", Fn: func(ctx context.Context) error { gs.GracefulStop(); return nil }},
		shutdown.Hook{Name: "kafka", Fn: func(ctx context.Context) error { return rt.Pub.Close() }},
		shutdown.Hook{Name: "db", Fn: func(ctx context.Context) error { rt.Pool.Close(); return nil }},
		shutdown.Hook{Name: "otel", Fn: rt.Obs.Shutdown},
	)
}

func serve(rt *boot.Runtime) {
	rt.Log.Info("listen", "addr", rt.Cfg.HTTPAddr)
	if err := rt.Server.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		rt.Log.Error("http", "err", err)
	}
}

type orders struct{ rt *boot.Runtime }

type item struct {
	SKU        string `json:"sku"`
	Quantity   int64  `json:"quantity"`
	UnitAmount int64  `json:"unit_amount"`
}

type createReq struct {
	Items      []item `json:"items"`
	Currency   string `json:"currency"`
	CustomerID string `json:"customer_id"`
}

type orderDTO struct {
	ID         string `json:"id"`
	CustomerID string `json:"customer_id"`
	Status     string `json:"status"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
	Items      []item `json:"items"`
	Version    int    `json:"version"`
}

func (s *orders) create(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req createReq
	if err := httpx.Decode(r, &req); err != nil || len(req.Items) == 0 {
		httpx.Problem(w, http.StatusBadRequest, "items required")
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	if req.CustomerID == "" {
		req.CustomerID = ident.UserID
	}
	var total int64
	for _, it := range req.Items {
		if it.Quantity < 1 || it.SKU == "" {
			httpx.Problem(w, http.StatusBadRequest, "invalid item")
			return
		}
		total += it.UnitAmount * it.Quantity
	}
	oid := id.NewULID()
	dto := orderDTO{ID: oid, CustomerID: req.CustomerID, Status: "pending", Amount: total, Currency: req.Currency, Items: req.Items, Version: 1}
	idem := r.Header.Get("Idempotency-Key")
	ctx := r.Context()
	err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if idem != "" {
			rec, err := idempotency.Begin(ctx, tx, ident.TenantID, idem, idempotency.Hash(req))
			if err != nil {
				return err
			}
			if rec != nil {
				return &replay{body: rec.Response}
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO orders (id, tenant_id, customer_id, status, amount, currency)
			VALUES ($1,$2,$3,'pending',$4,$5)`, oid, ident.TenantID, req.CustomerID, total, req.Currency); err != nil {
			return err
		}
		for _, it := range req.Items {
			if _, err := tx.Exec(ctx, `INSERT INTO order_items (order_id, sku, quantity, unit_amount) VALUES ($1,$2,$3,$4)`,
				oid, it.SKU, it.Quantity, it.UnitAmount); err != nil {
				return err
			}
		}
		if err := s.emit(ctx, tx, events.OrderCreated, dto, ""); err != nil {
			return err
		}
		if idem != "" {
			return idempotency.Complete(ctx, tx, ident.TenantID, idem, dto)
		}
		return nil
	})
	var rp *replay
	if errors.As(err, &rp) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(rp.body)
		return
	}
	if errors.Is(err, idempotency.ErrConflict) {
		httpx.Problem(w, http.StatusUnprocessableEntity, "idempotency conflict")
		return
	}
	if errors.Is(err, idempotency.ErrInFlight) {
		httpx.Problem(w, http.StatusConflict, "in flight")
		return
	}
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusCreated, dto)
}

func (s *orders) get(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	dto, err := s.load(r.Context(), ident.TenantID, chi.URLParam(r, "id"))
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "order")
		return
	}
	httpx.JSON(w, http.StatusOK, dto)
}

func (s *orders) transition(to, eventType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ident := tenant.MustFrom(r.Context())
		oid := chi.URLParam(r, "id")
		ctx := r.Context()
		var dto orderDTO
		err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
			var err error
			dto, err = s.loadTx(ctx, tx, ident.TenantID, oid)
			if err != nil {
				return err
			}
			if !legal(dto.Status, to) {
				return errIllegal
			}
			if _, err := tx.Exec(ctx, `UPDATE orders SET status=$1, version=version+1, updated_at=now() WHERE id=$2 AND tenant_id=$3`, to, oid, ident.TenantID); err != nil {
				return err
			}
			dto.Status = to
			dto.Version++
			return s.emit(ctx, tx, eventType, dto, "")
		})
		if errors.Is(err, errIllegal) {
			httpx.Problem(w, http.StatusConflict, "illegal transition")
			return
		}
		if err != nil {
			httpx.Problem(w, http.StatusNotFound, "order")
			return
		}
		httpx.JSON(w, http.StatusOK, dto)
	}
}

func legal(from, to string) bool {
	switch from + "->" + to {
	case "pending->confirmed", "pending->cancelled", "pending->failed", "confirmed->cancelled":
		return true
	default:
		return false
	}
}

func (s *orders) load(ctx context.Context, tid, oid string) (orderDTO, error) {
	return s.loadTx(ctx, s.rt.Pool, tid, oid)
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *orders) loadTx(ctx context.Context, q querier, tid, oid string) (orderDTO, error) {
	var d orderDTO
	err := q.QueryRow(ctx, `SELECT id, customer_id, status, amount, currency, version FROM orders WHERE id=$1 AND tenant_id=$2`, oid, tid).
		Scan(&d.ID, &d.CustomerID, &d.Status, &d.Amount, &d.Currency, &d.Version)
	if err != nil {
		return d, err
	}
	rows, err := q.Query(ctx, `SELECT sku, quantity, unit_amount FROM order_items WHERE order_id=$1`, oid)
	if err != nil {
		return d, err
	}
	defer rows.Close()
	for rows.Next() {
		var it item
		if err := rows.Scan(&it.SKU, &it.Quantity, &it.UnitAmount); err != nil {
			return d, err
		}
		d.Items = append(d.Items, it)
	}
	return d, nil
}

func (s *orders) emit(ctx context.Context, tx pgx.Tx, typ string, d orderDTO, reason string) error {
	items := make([]events.OrderItem, 0, len(d.Items))
	for _, it := range d.Items {
		items = append(items, events.OrderItem{SKU: it.SKU, Quantity: it.Quantity, UnitAmount: it.UnitAmount})
	}
	env, err := events.New(ctx, typ, "order", "order", d.ID, events.OrderPayload{
		OrderID: d.ID, CustomerID: d.CustomerID, Status: d.Status, Amount: d.Amount, Currency: d.Currency, Items: items, Reason: reason,
	})
	if err != nil {
		return err
	}
	return outbox.Insert(ctx, tx, env)
}

type replay struct{ body []byte }

func (r *replay) Error() string { return "idempotent replay" }

var errIllegal = errors.New("illegal")
