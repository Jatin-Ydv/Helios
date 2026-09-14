package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "orchestrator", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc, err := newOrchestrator(ctx, rt)
	if err != nil {
		rt.Log.Error("grpc dial", "err", err)
		os.Exit(1)
	}
	rt.Server.Router.Route("/v1/checkout", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/", svc.checkout)
		r.Get("/{id}", svc.get)
	})
	rt.StartOutboxRelay(ctx)
	go svc.timeoutLoop(ctx)
	go serve(rt)
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { cancel(); return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "grpc-clients", Fn: func(ctx context.Context) error {
			for _, c := range svc.conns {
				_ = c.Close()
			}
			return nil
		}},
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

type orchestrator struct {
	rt    *boot.Runtime
	tok   string
	order heliosv1.OrderServiceClient
	inv   heliosv1.InventoryServiceClient
	pay   heliosv1.PaymentServiceClient
	ful   heliosv1.FulfillmentServiceClient
	conns []*grpc.ClientConn
}

func newOrchestrator(ctx context.Context, rt *boot.Runtime) (*orchestrator, error) {
	o := &orchestrator{rt: rt, tok: rt.InternalToken()}
	targets := []struct {
		env, def string
		set      func(*grpc.ClientConn)
	}{
		{"ORDER_GRPC", "order:9090", func(c *grpc.ClientConn) { o.order = heliosv1.NewOrderServiceClient(c) }},
		{"INVENTORY_GRPC", "inventory:9090", func(c *grpc.ClientConn) { o.inv = heliosv1.NewInventoryServiceClient(c) }},
		{"PAYMENT_GRPC", "payment:9090", func(c *grpc.ClientConn) { o.pay = heliosv1.NewPaymentServiceClient(c) }},
		{"FULFILLMENT_GRPC", "fulfillment:9090", func(c *grpc.ClientConn) { o.ful = heliosv1.NewFulfillmentServiceClient(c) }},
	}
	for _, t := range targets {
		addr := envURL(t.env, t.def)
		dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		conn, err := grpcx.Dial(dctx, addr)
		cancel()
		if err != nil {
			for _, c := range o.conns {
				_ = c.Close()
			}
			return nil, fmt.Errorf("%s (%s): %w", t.env, addr, err)
		}
		o.conns = append(o.conns, conn)
		t.set(conn)
		rt.Log.Info("grpc dialed", "target", addr)
	}
	return o, nil
}

func envURL(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

type line struct {
	SKU        string `json:"sku"`
	Quantity   int64  `json:"quantity"`
	UnitAmount int64  `json:"unit_amount"`
}

type checkoutReq struct {
	Items    []line `json:"items"`
	Currency string `json:"currency"`
}

type sagaView struct {
	ID      string `json:"id"`
	OrderID string `json:"order_id"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
}

func (o *orchestrator) checkout(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req checkoutReq
	if err := httpx.Decode(r, &req); err != nil || len(req.Items) == 0 {
		httpx.Problem(w, http.StatusBadRequest, "items required")
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	sid := id.NewULID()
	ctx := tenant.With(r.Context(), ident)
	body, _ := json.Marshal(req)
	_, err := o.rt.Pool.Exec(ctx, `
		INSERT INTO sagas (id, tenant_id, order_id, customer_id, state, payload, expires_at)
		VALUES ($1,$2,'', $3, 'started', $4, now() + interval '45 seconds')`,
		sid, ident.TenantID, ident.UserID, body)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "saga persist")
		return
	}
	o.rt.Metrics.SagaActive.Inc()

	view, err := o.execute(ctx, ident, sid, req)
	if err != nil {
		o.rt.Metrics.SagaFailed.Inc()
		o.rt.Metrics.SagaActive.Dec()
		httpx.JSON(w, http.StatusConflict, view)
		return
	}
	o.rt.Metrics.SagaActive.Dec()
	httpx.JSON(w, http.StatusCreated, view)
}

func (o *orchestrator) execute(ctx context.Context, ident tenant.Identity, sid string, req checkoutReq) (sagaView, error) {
	gctx := grpcx.Outgoing(ctx, o.tok, ident)
	items := make([]*heliosv1.LineItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, &heliosv1.LineItem{Sku: it.SKU, Quantity: it.Quantity, UnitAmount: it.UnitAmount})
	}
	created, err := o.order.CreateOrder(gctx, &heliosv1.CreateOrderRequest{
		Items: items, Currency: req.Currency, CustomerId: ident.UserID,
	})
	if err != nil {
		return o.fail(ctx, ident, sid, "", "create_order", fmt.Sprintf("order create: %v", err))
	}
	_, _ = o.rt.Pool.Exec(ctx, `UPDATE sagas SET order_id=$1, state='order_created', updated_at=now() WHERE id=$2`, created.OrderId, sid)
	o.step(ctx, sid, "create_order", "ok", created)

	for _, it := range req.Items {
		_, err = o.inv.Reserve(gctx, &heliosv1.ReserveRequest{
			OrderId: created.OrderId, Sku: it.SKU, Quantity: it.Quantity,
		})
		if err != nil {
			o.compensate(ctx, ident, created.OrderId, req.Items, "", true, false)
			return o.fail(ctx, ident, sid, created.OrderId, "reserve", "inventory reservation failed")
		}
	}
	_, _ = o.rt.Pool.Exec(ctx, `UPDATE sagas SET state='inventory_reserved', updated_at=now() WHERE id=$1`, sid)
	o.step(ctx, sid, "reserve", "ok", nil)

	pay, err := o.pay.Authorize(gctx, &heliosv1.AuthorizeRequest{
		OrderId: created.OrderId, Amount: created.Amount, Currency: req.Currency,
	})
	if err != nil || pay.GetStatus() != "authorized" {
		pid := ""
		if pay != nil {
			pid = pay.PaymentId
		}
		o.compensate(ctx, ident, created.OrderId, req.Items, pid, true, false)
		return o.fail(ctx, ident, sid, created.OrderId, "authorize", "payment declined or unavailable")
	}
	_, _ = o.rt.Pool.Exec(ctx, `UPDATE sagas SET state='payment_authorized', updated_at=now() WHERE id=$1`, sid)
	o.step(ctx, sid, "authorize", "ok", pay)

	if _, err = o.order.ConfirmOrder(gctx, &heliosv1.OrderIDRequest{OrderId: created.OrderId}); err != nil {
		o.compensate(ctx, ident, created.OrderId, req.Items, pay.PaymentId, true, true)
		return o.fail(ctx, ident, sid, created.OrderId, "confirm", "order confirm failed")
	}
	_, _ = o.rt.Pool.Exec(ctx, `UPDATE sagas SET state='order_confirmed', updated_at=now() WHERE id=$1`, sid)

	_, _ = o.ful.Allocate(gctx, &heliosv1.AllocateRequest{OrderId: created.OrderId})

	err = database.InTx(ctx, o.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE sagas SET state='completed', updated_at=now() WHERE id=$1`, sid); err != nil {
			return err
		}
		env, err := events.New(ctx, events.SagaCompleted, "orchestrator", "saga", sid, events.SagaPayload{
			SagaID: sid, OrderID: created.OrderId, Status: "completed",
		})
		if err != nil {
			return err
		}
		nenv, err := events.New(ctx, events.NotificationRequested, "orchestrator", "saga", sid, events.NotificationPayload{
			Channel: "email", To: ident.UserID, Template: "order_confirmed",
			Data: map[string]any{"order_id": created.OrderId, "saga_id": sid},
		})
		if err != nil {
			return err
		}
		if err := outbox.Insert(ctx, tx, env); err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, nenv)
	})
	if err != nil {
		o.rt.Log.Error("saga complete persist", "err", err)
	}
	return sagaView{ID: sid, OrderID: created.OrderId, State: "completed"}, nil
}

func (o *orchestrator) compensate(ctx context.Context, ident tenant.Identity, orderID string, items []line, paymentID string, releaseInv, voidPay bool) {
	gctx := grpcx.Outgoing(ctx, o.tok, ident)
	if voidPay && paymentID != "" {
		_, _ = o.pay.Void(gctx, &heliosv1.VoidRequest{PaymentId: paymentID})
	}
	if releaseInv {
		for _, it := range items {
			_, _ = o.inv.Release(gctx, &heliosv1.ReleaseRequest{OrderId: orderID, Sku: it.SKU})
		}
	}
	if orderID != "" {
		_, _ = o.order.FailOrder(gctx, &heliosv1.OrderIDRequest{OrderId: orderID})
	}
}

func (o *orchestrator) fail(ctx context.Context, ident tenant.Identity, sid, orderID, step, reason string) (sagaView, error) {
	o.step(ctx, sid, step, "failed", reason)
	_ = database.InTx(ctx, o.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE sagas SET state='compensated', reason=$1, updated_at=now() WHERE id=$2`, reason, sid)
		if err != nil {
			return err
		}
		env, err := events.New(ctx, events.SagaCompensated, "orchestrator", "saga", sid, events.SagaPayload{
			SagaID: sid, OrderID: orderID, Status: "compensated", Reason: reason,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	return sagaView{ID: sid, OrderID: orderID, State: "compensated", Reason: reason}, errors.New(reason)
}

func (o *orchestrator) step(ctx context.Context, sid, name, status string, detail any) {
	b, _ := json.Marshal(detail)
	_, _ = o.rt.Pool.Exec(ctx, `INSERT INTO saga_steps (saga_id, name, status, detail) VALUES ($1,$2,$3,$4)
		ON CONFLICT (saga_id, name) DO UPDATE SET status=EXCLUDED.status, detail=EXCLUDED.detail`, sid, name, status, b)
}

func (o *orchestrator) get(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var view sagaView
	err := o.rt.Pool.QueryRow(r.Context(), `SELECT id, order_id, state, COALESCE(reason,'') FROM sagas WHERE id=$1 AND tenant_id=$2`,
		chi.URLParam(r, "id"), ident.TenantID).Scan(&view.ID, &view.OrderID, &view.State, &view.Reason)
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "saga")
		return
	}
	httpx.JSON(w, http.StatusOK, view)
}

func (o *orchestrator) timeoutLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			rows, err := o.rt.Pool.Query(ctx, `
				SELECT id, tenant_id, order_id, customer_id, payload FROM sagas
				WHERE state NOT IN ('completed','compensated') AND expires_at < now()
				LIMIT 20`)
			if err != nil {
				continue
			}
			type stuck struct {
				id, tid, oid, cid string
				payload           []byte
			}
			var list []stuck
			for rows.Next() {
				var s stuck
				if rows.Scan(&s.id, &s.tid, &s.oid, &s.cid, &s.payload) == nil {
					list = append(list, s)
				}
			}
			rows.Close()
			for _, s := range list {
				var req checkoutReq
				_ = json.Unmarshal(s.payload, &req)
				ident := tenant.Identity{TenantID: s.tid, UserID: s.cid, Role: "service"}
				o.compensate(ctx, ident, s.oid, req.Items, "", true, true)
				_, _ = o.fail(ctx, ident, s.id, s.oid, "timeout", "saga timeout")
				o.rt.Metrics.SagaFailed.Inc()
			}
		}
	}
}
