package resilience

import (
	"context"
	"errors"
)

var ErrBulkheadFull = errors.New("bulkhead: no capacity")

// Bulkhead caps concurrent in-flight calls to a dependency (cabin isolation).
// Combined with a breaker, it prevents a slow PSP from exhausting the order
// service's goroutines — the classic cascading-failure mode.
type Bulkhead struct {
	sem chan struct{}
}

func NewBulkhead(limit int) *Bulkhead {
	if limit < 1 {
		limit = 32
	}
	return &Bulkhead{sem: make(chan struct{}, limit)}
}

func (b *Bulkhead) Execute(ctx context.Context, fn func() error) error {
	select {
	case b.sem <- struct{}{}:
		defer func() { <-b.sem }()
		return fn()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrBulkheadFull
	}
}
