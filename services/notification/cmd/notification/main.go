package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/kafka"
	"github.com/helios-commerce/helios/platform/shutdown"
	"github.com/helios-commerce/helios/platform/tenant"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt, err := boot.New(ctx, "notification", true, false, true)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	svc := &notifier{rt: rt}
	rt.Server.Router.Route("/v1/notifications", func(r httpx.Router) {
		r.Use(httpx.AuthFlexible(rt.JWT, rt.InternalToken()))
		r.Get("/", svc.list)
	})
	cons := kafka.NewConsumer(rt.Cfg.KafkaBrokers, "helios.notification.commands", "notification-dispatch", "notification", rt.Log, rt.Metrics, svc.onCommand)
	orderC := kafka.NewConsumer(rt.Cfg.KafkaBrokers, "helios.order.events", "notification-order", "notification", rt.Log, rt.Metrics, svc.onOrder)
	go func() { _ = cons.Run(ctx) }()
	go func() { _ = orderC.Run(ctx) }()
	go serve(rt)
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { cancel(); return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "c1", Fn: func(ctx context.Context) error { return cons.Close() }},
		shutdown.Hook{Name: "c2", Fn: func(ctx context.Context) error { return orderC.Close() }},
		shutdown.Hook{Name: "kafka-p", Fn: func(ctx context.Context) error { return rt.Pub.Close() }},
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

type notifier struct{ rt *boot.Runtime }

func (n *notifier) onCommand(ctx context.Context, env events.Envelope) error {
	var p events.NotificationPayload
	if err := env.Decode(&p); err != nil {
		return err
	}
	return n.persist(ctx, env, p)
}

func (n *notifier) onOrder(ctx context.Context, env events.Envelope) error {
	if env.EventType != events.OrderConfirmed && env.EventType != events.OrderCancelled {
		return nil
	}
	var p events.OrderPayload
	if err := env.Decode(&p); err != nil {
		return err
	}
	return n.persist(ctx, env, events.NotificationPayload{
		Channel:  "email",
		To:       p.CustomerID,
		Template: env.EventType,
		Data:     map[string]any{"order_id": p.OrderID, "status": p.Status},
	})
}

func (n *notifier) persist(ctx context.Context, env events.Envelope, p events.NotificationPayload) error {
	body, _ := json.Marshal(p)
	_, err := n.rt.Pool.Exec(ctx, `
		INSERT INTO notifications (id, tenant_id, channel, recipient, template, payload, delivered_at)
		VALUES ($1,$2,$3,$4,$5,$6, now())
		ON CONFLICT (id) DO NOTHING`, env.EventID, env.TenantID, p.Channel, p.To, p.Template, body)
	if err != nil {
		return err
	}
	n.rt.Log.Info("notification dispatched",
		"channel", p.Channel, "template", p.Template, "to", p.To, "event_id", env.EventID)
	return nil
}

func (n *notifier) list(w http.ResponseWriter, r *http.Request) {
	ident := tenant.MustFrom(r.Context())
	rows, err := n.rt.Pool.Query(r.Context(), `
		SELECT id, channel, recipient, template, delivered_at
		FROM notifications WHERE tenant_id=$1 ORDER BY delivered_at DESC LIMIT 50`, ident.TenantID)
	if err != nil {
		httpx.Problem(w, http.StatusInternalServerError, "query")
		return
	}
	defer rows.Close()
	type row struct {
		ID        string    `json:"id"`
		Channel   string    `json:"channel"`
		Recipient string    `json:"recipient"`
		Template  string    `json:"template"`
		At        time.Time `json:"delivered_at"`
	}
	out := []row{}
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.ID, &x.Channel, &x.Recipient, &x.Template, &x.At); err != nil {
			httpx.Problem(w, http.StatusInternalServerError, "scan")
			return
		}
		out = append(out, x)
	}
	httpx.JSON(w, http.StatusOK, map[string]any{"items": out})
}
