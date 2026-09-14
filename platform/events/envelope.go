// Package events defines the canonical CloudEvents-inspired envelope used on
// every Kafka topic. Versioning, correlation, and trace propagation live here
// so producers cannot "forget" them.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/tenant"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const SchemaVersion = 1

// Well-known event types. Changing a string is a breaking contract change —
// add a new type (and version) instead of mutating an existing one.
const (
	ProductUpserted        = "catalog.product.upserted.v1"
	ProductDeleted         = "catalog.product.deleted.v1"
	InventoryReserved      = "inventory.reservation.reserved.v1"
	InventoryReservationFailed = "inventory.reservation.failed.v1"
	InventoryReleased      = "inventory.reservation.released.v1"
	InventoryCommitted     = "inventory.reservation.committed.v1"
	OrderCreated           = "order.created.v1"
	OrderConfirmed         = "order.confirmed.v1"
	OrderCancelled         = "order.cancelled.v1"
	OrderFailed            = "order.failed.v1"
	PaymentAuthorized      = "payment.authorized.v1"
	PaymentAuthorizationFailed = "payment.authorization.failed.v1"
	PaymentCaptured        = "payment.captured.v1"
	PaymentVoided          = "payment.voided.v1"
	FulfillmentAllocated   = "fulfillment.allocated.v1"
	FulfillmentShipped     = "fulfillment.shipped.v1"
	FulfillmentFailed      = "fulfillment.failed.v1"
	NotificationRequested  = "notification.requested.v1"
	SagaCompleted          = "saga.checkout.completed.v1"
	SagaCompensated        = "saga.checkout.compensated.v1"
)

type Envelope struct {
	SpecVersion   int             `json:"spec_version"`
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	TenantID      string          `json:"tenant_id"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Trace         map[string]string `json:"trace,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

func New(ctx context.Context, eventType, producer, aggregateType, aggregateID string, payload any) (Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, err
	}
	tid := ""
	if ident, err := tenant.From(ctx); err == nil {
		tid = ident.TenantID
	}
	corr := CorrelationID(ctx)
	env := Envelope{
		SpecVersion:    SchemaVersion,
		EventID:        id.NewULID(),
		EventType:      eventType,
		EventVersion:   1,
		OccurredAt:     time.Now().UTC(),
		Producer:       producer,
		TenantID:       tid,
		CorrelationID:  corr,
		CausationID:    CausationID(ctx),
		AggregateType:  aggregateType,
		AggregateID:    aggregateID,
		IdempotencyKey: idempotencyFrom(ctx),
		Trace:          injectTrace(ctx),
		Payload:        body,
	}
	if env.CausationID == "" {
		env.CausationID = env.EventID
	}
	return env, nil
}

func (e Envelope) Bytes() ([]byte, error) { return json.Marshal(e) }

func Parse(b []byte) (Envelope, error) {
	var e Envelope
	err := json.Unmarshal(b, &e)
	return e, err
}

func (e Envelope) Decode(v any) error { return json.Unmarshal(e.Payload, v) }

func injectTrace(ctx context.Context) map[string]string {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	if len(carrier) == 0 {
		span := trace.SpanFromContext(ctx)
		if span.SpanContext().IsValid() {
			carrier["traceparent"] = spanContextToTraceparent(span.SpanContext())
		}
	}
	return carrier
}

func spanContextToTraceparent(sc trace.SpanContext) string {
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
}

type corrKey struct{}
type causeKey struct{}
type idemKey struct{}

func WithCorrelation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, corrKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	if v, ok := ctx.Value(corrKey{}).(string); ok {
		return v
	}
	return ""
}

func WithCausation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, causeKey{}, id)
}

func CausationID(ctx context.Context) string {
	if v, ok := ctx.Value(causeKey{}).(string); ok {
		return v
	}
	return ""
}

func WithIdempotency(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idemKey{}, key)
}

func idempotencyFrom(ctx context.Context) string {
	if v, ok := ctx.Value(idemKey{}).(string); ok {
		return v
	}
	return ""
}

// ExtractTrace returns a context with the remote span reconstructed from the envelope.
func ExtractTrace(ctx context.Context, e Envelope) context.Context {
	if e.Trace == nil {
		return ctx
	}
	return propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(e.Trace))
}
