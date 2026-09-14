package resilience

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
	// RetryIf decides whether an error is transient. Nil retries every error.
	RetryIf func(error) bool
}

func DefaultRetry() RetryPolicy {
	return RetryPolicy{MaxAttempts: 5, Base: 50 * time.Millisecond, Max: 2 * time.Second}
}

func Do(ctx context.Context, p RetryPolicy, fn func(attempt int) error) error {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	var err error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn(attempt)
		if err == nil {
			return nil
		}
		if p.RetryIf != nil && !p.RetryIf(err) {
			return err
		}
		if attempt == p.MaxAttempts {
			return err
		}
		delay := backoff(p.Base, p.Max, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func backoff(base, max time.Duration, attempt int) time.Duration {
	exp := float64(base) * math.Pow(2, float64(attempt-1))
	if exp > float64(max) {
		exp = float64(max)
	}
	// Full jitter: random in [0, exp] — AWS architecture blog, "Exponential
	// Backoff And Jitter". Prevents synchronized retry storms.
	var b [8]byte
	_, _ = rand.Read(b[:])
	r := binary.LittleEndian.Uint64(b[:])
	j := time.Duration(r % uint64(exp+1))
	return j
}

func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var te interface{ Temporary() bool }
	if errors.As(err, &te) {
		return te.Temporary()
	}
	return true
}
