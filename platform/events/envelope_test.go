package events_test

import (
	"context"
	"testing"

	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/tenant"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	ctx := tenant.With(context.Background(), tenant.Identity{TenantID: "t1", UserID: "u1"})
	ctx = events.WithCorrelation(ctx, "corr-1")
	env, err := events.New(ctx, events.OrderCreated, "order", "order", "ord_1", events.OrderPayload{
		OrderID: "ord_1", Status: "pending", Amount: 100, Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	if env.TenantID != "t1" || env.CorrelationID != "corr-1" {
		t.Fatalf("tenant/corr missing: %+v", env)
	}
	raw, err := env.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	got, err := events.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	var p events.OrderPayload
	if err := got.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.OrderID != "ord_1" {
		t.Fatalf("payload %+v", p)
	}
}
