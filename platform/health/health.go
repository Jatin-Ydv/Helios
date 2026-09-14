package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

type Status string

const (
	Up   Status = "up"
	Down Status = "down"
)

type Check func(ctx context.Context) error

type Checker struct {
	mu     sync.RWMutex
	checks map[string]Check
	ready  bool
}

func New() *Checker { return &Checker{checks: map[string]Check{}, ready: true} }

func (c *Checker) Register(name string, fn Check) {
	c.mu.Lock()
	c.checks[name] = fn
	c.mu.Unlock()
}

func (c *Checker) SetReady(v bool) {
	c.mu.Lock()
	c.ready = v
	c.mu.Unlock()
}

func (c *Checker) Live() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

func (c *Checker) Ready() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.RLock()
		ready := c.ready
		checks := make(map[string]Check, len(c.checks))
		for k, v := range c.checks {
			checks[k] = v
		}
		c.mu.RUnlock()

		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		report := map[string]string{}
		ok := true
		for name, fn := range checks {
			if err := fn(ctx); err != nil {
				ok = false
				report[name] = err.Error()
			} else {
				report[name] = string(Up)
			}
		}
		code := http.StatusOK
		status := Up
		if !ok {
			code = http.StatusServiceUnavailable
			status = Down
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "checks": report})
	}
}
