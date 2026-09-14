package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/config"
	"github.com/helios-commerce/helios/platform/database"
	"github.com/helios-commerce/helios/platform/events"
	heliosv1 "github.com/helios-commerce/helios/gen/helios/v1"
	"github.com/helios-commerce/helios/platform/grpcx"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/outbox"
	"github.com/helios-commerce/helios/platform/resilience"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
	"github.com/jackc/pgx/v5"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "payment", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	cb := resilience.NewBreaker("psp", 5, 8*time.Second, func(name string, st resilience.State) {
		rt.Log.Warn("circuit", "name", name, "state", st.String())
		var v float64
		switch st {
		case resilience.HalfOpen:
			v = 1
		case resilience.Open:
			v = 2
		}
		rt.Metrics.CBState.WithLabelValues(name).Set(v)
	})
	bh := resilience.NewBulkhead(16)
	svc := &payments{rt: rt, cb: cb, bh: bh, psp: &simulatedPSP{
		failRate: config.EnvInt("PSP_FAILURE_RATE_PCT", 0),
		latency:  time.Duration(config.EnvInt("PSP_LATENCY_MS", 40)) * time.Millisecond,
	}}
	rt.Server.Router.Route("/v1/payments", func(r chi.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Post("/authorize", svc.authorize)
		r.Post("/{id}/capture", svc.capture)
		r.Post("/{id}/void", svc.void)
		r.Get("/by-order/{orderId}", svc.byOrder)
	})
	rt.StartOutboxRelay(ctx)
	gs := grpcx.NewServer(rt.InternalToken())
	heliosv1.RegisterPaymentServiceServer(gs, &paymentGRPC{svc: svc})
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

type payments struct {
	rt  *boot.Runtime
	cb  *resilience.Breaker
	bh  *resilience.Bulkhead
	psp *simulatedPSP
}

type authReq struct {
	OrderID  string `json:"order_id"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

func (s *payments) authorize(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var req authReq
	if err := httpx.Decode(r, &req); err != nil || req.OrderID == "" || req.Amount < 1 {
		httpx.Problem(w, http.StatusBadRequest, "order_id and amount required")
		return
	}
	if req.Currency == "" {
		req.Currency = "USD"
	}
	pid := id.NewULID()
	ctx := r.Context()

	var pspRef string
	err := s.bh.Execute(ctx, func() error {
		return s.cb.Execute(func() error {
			return resilience.Do(ctx, resilience.DefaultRetry(), func(int) error {
				var e error
				pspRef, e = s.psp.Authorize(ctx, req.OrderID, req.Amount)
				return e
			})
		})
	})
	status := "authorized"
	ev := events.PaymentAuthorized
	reason := ""
	if errors.Is(err, resilience.ErrOpen) || errors.Is(err, resilience.ErrBulkheadFull) {
		httpx.Problem(w, http.StatusServiceUnavailable, "payment dependency unavailable")
		return
	}
	if err != nil {
		status = "failed"
		ev = events.PaymentAuthorizationFailed
		reason = err.Error()
	}
	err = database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			INSERT INTO payments (id, tenant_id, order_id, amount, currency, status, psp_reference)
			VALUES ($1,$2,$3,$4,$5,$6,$7)
			ON CONFLICT (tenant_id, order_id) DO NOTHING`,
			pid, ident.TenantID, req.OrderID, req.Amount, req.Currency, status, pspRef); err != nil {
			return err
		}
		env, err := events.New(ctx, ev, "payment", "payment", pid, events.PaymentPayload{
			PaymentID: pid, OrderID: req.OrderID, Amount: req.Amount, Currency: req.Currency, Status: status, PSPReference: pspRef, Reason: reason,
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
	code := http.StatusOK
	if status == "failed" {
		code = http.StatusPaymentRequired
	}
	httpx.JSON(w, code, map[string]any{"payment_id": pid, "status": status, "psp_reference": pspRef, "reason": reason})
}

func (s *payments) capture(w http.ResponseWriter, r *http.Request) {
	s.finish(w, r, "captured", events.PaymentCaptured)
}
func (s *payments) void(w http.ResponseWriter, r *http.Request) {
	s.finish(w, r, "voided", events.PaymentVoided)
}

func (s *payments) finish(w http.ResponseWriter, r *http.Request, status, ev string) {
	ident := tenant.MustFrom(r.Context())
	pid := chi.URLParam(r, "id")
	ctx := r.Context()
	var orderID string
	err := database.InTx(ctx, s.rt.Pool, func(ctx context.Context, tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE payments SET status=$1
			WHERE id=$2 AND tenant_id=$3 AND status='authorized'
			RETURNING order_id`, status, pid, ident.TenantID).Scan(&orderID)
		if err != nil {
			return err
		}
		env, err := events.New(ctx, ev, "payment", "payment", pid, events.PaymentPayload{
			PaymentID: pid, OrderID: orderID, Status: status,
		})
		if err != nil {
			return err
		}
		return outbox.Insert(ctx, tx, env)
	})
	if err != nil {
		httpx.Problem(w, http.StatusConflict, "not capturable")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"payment_id": pid, "status": status})
}

func (s *payments) byOrder(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	var pid, status, ref string
	var amount int64
	err := s.rt.Pool.QueryRow(r.Context(), `
		SELECT id, status, amount, COALESCE(psp_reference,'') FROM payments
		WHERE tenant_id=$1 AND order_id=$2`, ident.TenantID, chi.URLParam(r, "orderId")).
		Scan(&pid, &status, &amount, &ref)
	if err != nil {
		httpx.Problem(w, http.StatusNotFound, "payment")
		return
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"payment_id": pid, "status": status, "amount": amount, "psp_reference": ref})
}

type simulatedPSP struct {
	failRate int
	latency  time.Duration
}

func (p *simulatedPSP) Authorize(ctx context.Context, orderID string, amount int64) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(p.latency):
	}
	if p.failRate > 0 && rand.Intn(100) < p.failRate {
		return "", errors.New("psp declined")
	}
	return "psp_" + id.NewUUIDv7(), nil
}
