package resilience

import (
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit breaker is open")

type State int

const (
	Closed State = iota
	HalfOpen
	Open
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case HalfOpen:
		return "half-open"
	default:
		return "open"
	}
}

// Breaker is a count-based circuit breaker with a half-open probe.
//
// Why not Hystrix command keys everywhere: a breaker is a *dependency*
// isolation tool, not a method decorator. One breaker per downstream (payment
// PSP, inventory HTTP, Elasticsearch). Mixing them hides which hop is sick.
type Breaker struct {
	name         string
	maxFailures  int
	resetTimeout time.Duration
	halfOpenMax  int

	mu            sync.Mutex
	state         State
	failures      int
	openedAt      time.Time
	halfOpenCount int
	onState       func(name string, s State)
}

func NewBreaker(name string, maxFailures int, reset time.Duration, onState func(string, State)) *Breaker {
	if maxFailures < 1 {
		maxFailures = 5
	}
	if reset == 0 {
		reset = 10 * time.Second
	}
	return &Breaker{
		name:         name,
		maxFailures:  maxFailures,
		resetTimeout: reset,
		halfOpenMax:  1,
		onState:      onState,
	}
}

func (b *Breaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if time.Since(b.openedAt) > b.resetTimeout {
			b.transition(HalfOpen)
			b.halfOpenCount = 1
			return nil
		}
		return ErrOpen
	case HalfOpen:
		if b.halfOpenCount >= b.halfOpenMax {
			return ErrOpen
		}
		b.halfOpenCount++
		return nil
	default:
		return nil
	}
}

func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == HalfOpen {
		b.transition(Closed)
	}
	b.failures = 0
}

func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || b.failures >= b.maxFailures {
		b.transition(Open)
		b.openedAt = time.Now()
	}
}

func (b *Breaker) Execute(fn func() error) error {
	if err := b.Allow(); err != nil {
		return err
	}
	err := fn()
	if err != nil {
		b.Failure()
		return err
	}
	b.Success()
	return nil
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) transition(s State) {
	if b.state == s {
		return
	}
	b.state = s
	if s == Closed {
		b.failures = 0
		b.halfOpenCount = 0
	}
	if b.onState != nil {
		b.onState(b.name, s)
	}
}
