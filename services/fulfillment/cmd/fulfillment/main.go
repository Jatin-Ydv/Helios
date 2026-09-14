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
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "fulfillment", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc := &fulfillment{rt: rt}
	rt.Server.Router.Route("/v1/fulfillment", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/allocate", svc.allocate)
		r.Post("/{id}/ship", svc.ship)
		r.Get("/by-order/{orderId}", svc.byOrder)
	})
	rt.StartOutboxRelay(ctx)
	gs := grpcx.NewServer(rt.InternalToken())
	heliosv1.RegisterFulfillmentServiceServer(gs, &fulfillmentGRPC{svc: svc})
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

type fulfillment struct{ rt *boot.Runtime }

type allocReq struct {
	OrderID     string `json:"order_id"`
	WarehouseID string `json:"warehouse_id"`
}

func (s *fulfillment) allocate(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req allocReq
	if err := httpx.Decode(r, &req); err != nil || req.OrderID == "" {
		httpx.Problem(w, http.StatusBadRequest, "order_id required")
		return
	}
	if req.WarehouseID == "" {
		req.WarehouseID = "wh-default"
	}
	sid := id.NewULID()
	ctx := r.Context()
	err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO shipments (id, tenant_id, order_id, warehouse_id, status)
			VALUES ($1,$2,$3,$4,'allocated')
			ON CONFLICT (tenant_id, order_id) DO NOTHING`, sid, ident.TenantID, req.OrderID, req.WarehouseID); err != nil {
			return err
		}
		env, err := events.New(ctx, events.FulfillmentAllocated, "fulfillment", "shipment", sid, events.FulfillmentPayload{
			ShipmentID: sid, OrderID: req.OrderID, WarehouseID: req.WarehouseID,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, err.Error())
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"shipment_id": sid, "status": "allocated"})
}

func (s *fulfillment) ship(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	sid := chi.URLParam(r, "id")
	tracking := "1Z" + id.NewULID()[:12]
	ctx := r.Context()
	var orderID string
	err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE shipments SET status='shipped', carrier='helios-logistics', tracking=$1
			WHERE id=$2 AND tenant_id=$3 AND status='allocated' RETURNING order_id`,
			tracking, sid, ident.TenantID).Scan(&orderID)
		if err != nil {
			return err
		}
		env, err := events.New(ctx, events.FulfillmentShipped, "fulfillment", "shipment", sid, events.FulfillmentPayload{
			ShipmentID: sid, OrderID: orderID, Carrier: "helios-logistics", Tracking: tracking,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "not allocatable")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"shipment_id": sid, "tracking": tracking, "status": "shipped"})
}

func (s *fulfillment) byOrder(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var sid, status, tracking string
	err := s.rt.Pool.QueryRow(r.Context(), `
		SELECT id, status, COALESCE(tracking,'') FROM shipments WHERE tenant_id=$1 AND order_id=$2`,
		ident.TenantID, chi.URLParam(r, "orderId")).Scan(&sid, &status, &tracking)
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "shipment")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"shipment_id": sid, "status": status, "tracking": tracking})
}
