package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/redisx"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "inventory", true, true, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc := &inventory{rt: rt}
	rt.Server.Router.Route("/v1/inventory", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/stock", svc.upsertStock)
		r.Get("/{sku}", svc.get)
		r.Post("/reserve", svc.reserve)
		r.Post("/release", svc.release)
		r.Post("/commit", svc.commit)
	})
	rt.StartOutboxRelay(ctx)
	gs := grpcx.NewServer(rt.InternalToken())
	heliosv1.RegisterInventoryServiceServer(gs, &inventoryGRPC{svc: svc})
	go func() {
		if err := grpcx.ListenAndServe(rt.Log, rt.Cfg.GRPCAddr, gs); err != nil {
			rt.Log.Error("grpc", "err", err)
		}
	}()
	go svc.expireLoop(ctx)
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

type inventory struct{ rt *boot.Runtime }

type stockReq struct {
	WarehouseID string `json:"warehouse_id"`
	SKU         string `json:"sku"`
	OnHand      int64  `json:"on_hand"`
}

func (s *inventory) upsertStock(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req stockReq
	if err := httpx.Decode(r, &req); err != nil || req.SKU == "" {
		httpx.Problem(w, http.StatusBadRequest, "sku required")
		return
	}
	if req.WarehouseID == "" {
		req.WarehouseID = "wh-default"
	}
	_, err := s.rt.Pool.Exec(r.Context(), `
		INSERT INTO stock (tenant_id, warehouse_id, sku, on_hand)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (tenant_id, warehouse_id, sku)
		DO UPDATE SET on_hand = EXCLUDED.on_hand, version = stock.version + 1
	`, ident.TenantID, req.WarehouseID, req.SKU, req.OnHand)
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "stock constraint")
		return
	}
	httpx.JSON(w, http.StatusOK, req)
}

func (s *inventory) get(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	sku := chi.URLParam(r, "sku")
	wh := r.URL.Query().Get("warehouse_id")
	if wh == "" {
		wh = "wh-default"
	}
	var onHand, reserved int64
	var version int
	err := s.rt.Pool.QueryRow(r.Context(), `
		SELECT on_hand, reserved, version FROM stock
		WHERE tenant_id=$1 AND warehouse_id=$2 AND sku=$3`, ident.TenantID, wh, sku).
		Scan(&onHand, &reserved, &version)
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "sku")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{
		"sku": sku, "warehouse_id": wh, "on_hand": onHand, "reserved": reserved,
		"available": onHand - reserved, "version": version,
	})
}

type reserveReq struct {
	OrderID     string `json:"order_id"`
	SKU         string `json:"sku"`
	Quantity    int64  `json:"quantity"`
	WarehouseID string `json:"warehouse_id"`
}

func (s *inventory) reserve(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req reserveReq
	if err := httpx.Decode(r, &req); err != nil || req.Quantity < 1 || req.SKU == "" || req.OrderID == "" {
		httpx.Problem(w, http.StatusBadRequest, "order_id, sku, quantity required")
		return
	}
	if req.WarehouseID == "" {
		req.WarehouseID = "wh-default"
	}
	ctx := r.Context()
	lockKey := "inv:" + ident.TenantID + ":" + req.WarehouseID + ":" + req.SKU
	lock, err := redisx.Acquire(ctx, s.rt.Redis, lockKey, 3*time.Second)
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "reservation lock busy")
		return
	}
	defer func() { _ = lock.Unlock(context.Background()) }()

	fence, err := redisx.Fence(ctx, s.rt.Redis, lockKey)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "fence")
		return
	}
	resID := id.NewULID()
	err = database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var onHand, reserved int64
		if err := tx.QueryRow(ctx, `
			SELECT on_hand, reserved FROM stock
			WHERE tenant_id=$1 AND warehouse_id=$2 AND sku=$3
			FOR UPDATE`, ident.TenantID, req.WarehouseID, req.SKU).Scan(&onHand, &reserved); err != nil {
			return errStock
		}
		if onHand-reserved < req.Quantity {
			env, err := events.New(ctx, events.InventoryReservationFailed, "inventory", "reservation", resID, events.InventoryReservePayload{
				ReservationID: resID, OrderID: req.OrderID, SKU: req.SKU, WarehouseID: req.WarehouseID, Quantity: req.Quantity, Reason: "insufficient_stock",
			})
			if err != nil {
				return err
			}
			_ = outbox.Insert(ctx, tx, env)
			return errInsufficient
		}
		if _, err := tx.Exec(ctx, `
			UPDATE stock SET reserved = reserved + $1, version = version + 1
			WHERE tenant_id=$2 AND warehouse_id=$3 AND sku=$4`,
			req.Quantity, ident.TenantID, req.WarehouseID, req.SKU); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO reservations (id, tenant_id, order_id, warehouse_id, sku, quantity, status, fence, expires_at)
			VALUES ($1,$2,$3,$4,$5,$6,'pending',$7, now() + interval '15 minutes')
			ON CONFLICT (tenant_id, order_id, sku) DO NOTHING`,
			resID, ident.TenantID, req.OrderID, req.WarehouseID, req.SKU, req.Quantity, fence); err != nil {
			return err
		}
		env, err := events.New(ctx, events.InventoryReserved, "inventory", "reservation", resID, events.InventoryReservePayload{
			ReservationID: resID, OrderID: req.OrderID, SKU: req.SKU, WarehouseID: req.WarehouseID, Quantity: req.Quantity,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	switch {
	case errors.Is(err, errInsufficient):
		httpx.Problem(w, http.StatusConflict, "insufficient stock")
	case errors.Is(err, errStock):
		httpx.Problem(w, http.StatusNotFound, "unknown sku")
	case err != nil:
		httpx.Problem(w, http.StatusInternalServerError, err.Error())
	default:
		httpx.JSON(w, http.StatusOK, map[string]any{"reservation_id": resID, "status": "pending", "expires_in_seconds": 900})
	}
}

type mutateReq struct {
	OrderID string `json:"order_id"`
	SKU     string `json:"sku"`
}

func (s *inventory) release(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, "released", events.InventoryReleased, true)
}

func (s *inventory) commit(w http.ResponseWriter, r *http.Request) {
	s.mutate(w, r, "committed", events.InventoryCommitted, false)
}

func (s *inventory) mutate(w http.ResponseWriter, r *http.Request, status, eventType string, restoreReserved bool) {
	ident := tenant.MustFrom(r.Context())
	var req mutateReq
	if err := httpx.Decode(r, &req); err != nil {
		httpx.Problem(w, http.StatusBadRequest, "invalid json")
		return
	}
	ctx := r.Context()
	err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		var resID, sku, wh string
		var qty int64
		err := tx.QueryRow(ctx, `
			UPDATE reservations SET status=$1
			WHERE tenant_id=$2 AND order_id=$3 AND sku=$4 AND status='pending'
			RETURNING id, sku, warehouse_id, quantity`,
			status, ident.TenantID, req.OrderID, req.SKU).Scan(&resID, &sku, &wh, &qty)
		if err != nil {
			return err
		}
		if restoreReserved {
			_, err = tx.Exec(ctx, `
				UPDATE stock SET reserved = reserved - $1, version = version + 1
				WHERE tenant_id=$2 AND warehouse_id=$3 AND sku=$4`, qty, ident.TenantID, wh, sku)
		} else {
			_, err = tx.Exec(ctx, `
				UPDATE stock SET reserved = reserved - $1, on_hand = on_hand - $1, version = version + 1
				WHERE tenant_id=$2 AND warehouse_id=$3 AND sku=$4`, qty, ident.TenantID, wh, sku)
		}
		if err != nil {
			return err
		}
		env, err := events.New(ctx, eventType, "inventory", "reservation", resID, events.InventoryReservePayload{
			ReservationID: resID, OrderID: req.OrderID, SKU: sku, WarehouseID: wh, Quantity: qty,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "no pending reservation")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"status": status})
}

func (s *inventory) expireLoop(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, err := s.rt.Pool.Exec(ctx, `
				WITH expired AS (
					SELECT id, tenant_id, warehouse_id, sku, quantity
					FROM reservations
					WHERE status='pending' AND expires_at < now()
					FOR UPDATE SKIP LOCKED
					LIMIT 100
				), marked AS (
					UPDATE reservations r SET status='expired'
					FROM expired e WHERE r.id = e.id
					RETURNING e.tenant_id, e.warehouse_id, e.sku, e.quantity
				)
				UPDATE stock st
				SET reserved = GREATEST(0, reserved - m.quantity), version = st.version + 1
				FROM marked m
				WHERE st.tenant_id=m.tenant_id AND st.warehouse_id=m.warehouse_id AND st.sku=m.sku
			`)
			if err != nil {
				s.rt.Log.Error("expire", "err", err)
			}
		}
	}
}

var (
	errInsufficient = errors.New("insufficient")
	errStock        = errors.New("no stock row")
)
