package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Hook is a named shutdown step. Order is registration order (LIFO in Run).
// Typical order: stop accepting traffic → drain consumers → flush outbox →
// close pools → flush traces.
type Hook struct {
	Name string
	Fn   func(context.Context) error
}

func Run(log *slog.Logger, timeout time.Duration, hooks ...Hook) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	sig := <-ch
	log.Info("shutdown signal received", "signal", sig.String())

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	// Sequential LIFO: HTTP server must stop before we close the DB it uses.
	for i := len(hooks) - 1; i >= 0; i-- {
		h := hooks[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("shutdown hook start", "hook", h.Name)
			if err := h.Fn(ctx); err != nil {
				log.Error("shutdown hook failed", "hook", h.Name, "err", err)
			}
		}()
		wg.Wait()
	}
	log.Info("shutdown complete")
}
