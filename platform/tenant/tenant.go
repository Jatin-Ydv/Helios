// Package tenant carries multi-tenant identity on the request context.
//
// Helios is a logical multi-tenant platform. Isolation is enforced at:
//   - API gateway (JWT claim `tid`)
//   - every SQL query (tenant_id predicate — no cross-tenant SELECT)
//   - Kafka event envelope (tenant_id header, consumers reject mismatch)
//
// Physical isolation (one DB per tenant) is a deployment-time choice for
// enterprise SKUs; the code path is identical because tenant_id is always
// required, never inferred from "the" database.
package tenant

import (
	"context"
	"errors"
)

type ctxKey struct{}

var ErrMissing = errors.New("tenant: missing tenant id on context")

type Identity struct {
	TenantID string
	UserID   string
	Role     string
}

func With(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func From(ctx context.Context) (Identity, error) {
	v, ok := ctx.Value(ctxKey{}).(Identity)
	if !ok || v.TenantID == "" {
		return Identity{}, ErrMissing
	}
	return v, nil
}

func MustFrom(ctx context.Context) Identity {
	id, err := From(ctx)
	if err != nil {
		panic(err)
	}
	return id
}
