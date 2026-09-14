package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBreakerOpens(t *testing.T) {
	b := NewBreaker("psp", 3, 50*time.Millisecond, nil)
	fail := errors.New("boom")
	for i := 0; i < 3; i++ {
		_ = b.Execute(func() error { return fail })
	}
	if b.State() != Open {
		t.Fatalf("state %s", b.State())
	}
	if err := b.Allow(); err != ErrOpen {
		t.Fatalf("expected open, got %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if err := b.Allow(); err != nil {
		t.Fatalf("expected half-open probe, got %v", err)
	}
	b.Success()
	if b.State() != Closed {
		t.Fatalf("expected closed after probe success, got %s", b.State())
	}
}

func TestBulkheadFull(t *testing.T) {
	bh := NewBulkhead(1)
	entered := make(chan struct{})
	block := make(chan struct{})
	go func() {
		_ = bh.Execute(context.Background(), func() error {
			close(entered)
			<-block
			return nil
		})
	}()
	<-entered
	if err := bh.Execute(context.Background(), func() error { return nil }); err != ErrBulkheadFull {
		t.Fatalf("expected full, got %v", err)
	}
	close(block)
}
