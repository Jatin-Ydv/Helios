package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/helios-commerce/helios/platform/events"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, env events.Envelope) error
}

type TopicResolver func(eventType string) string

type Relay struct {
	Pool     *pgxpool.Pool
	Pub      Publisher
	Resolve  TopicResolver
	Log      *slog.Logger
	Interval time.Duration
	Batch    int
	OnLag    func(unpublished int64)
}

func (r *Relay) Run(ctx context.Context) {
	if r.Interval == 0 {
		r.Interval = 400 * time.Millisecond
	}
	if r.Batch == 0 {
		r.Batch = 50
	}
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.tick(ctx)
		}
	}
}

func (r *Relay) tick(ctx context.Context) {
	if r.OnLag != nil {
		if n, err := UnpublishedCount(ctx, r.Pool); err == nil {
			r.OnLag(n)
		}
	}
	batch, err := ClaimBatch(ctx, r.Pool, r.Batch)
	if err != nil {
		r.Log.Error("outbox claim", "err", err)
		return
	}
	for _, row := range batch {
		env, err := events.Parse(row.Payload)
		if err != nil {
			r.Log.Error("outbox parse", "id", row.ID, "err", err)
			continue
		}
		topic := r.Resolve(env.EventType)
		if err := r.Pub.Publish(ctx, topic, env); err != nil {
			r.Log.Warn("outbox publish", "id", row.ID, "tries", row.Tries, "err", err)
			continue
		}
		if err := MarkPublished(ctx, r.Pool, row.ID); err != nil {
			r.Log.Error("outbox mark", "id", row.ID, "err", err)
		}
	}
}

func DefaultTopics(eventType string) string {
	switch {
	case hasPrefix(eventType, "catalog."):
		return "helios.catalog.events"
	case hasPrefix(eventType, "inventory."):
		return "helios.inventory.events"
	case hasPrefix(eventType, "order."):
		return "helios.order.events"
	case hasPrefix(eventType, "payment."):
		return "helios.payment.events"
	case hasPrefix(eventType, "fulfillment."):
		return "helios.fulfillment.events"
	case hasPrefix(eventType, "notification."):
		return "helios.notification.commands"
	case hasPrefix(eventType, "saga."):
		return "helios.saga.events"
	default:
		return "helios.unknown.events"
	}
}

func hasPrefix(s, p string) bool {
	return len(s) >= len(p) && s[:len(p)] == p
}
