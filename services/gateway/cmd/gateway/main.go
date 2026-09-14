package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/helios-commerce/helios/platform/boot"
	"github.com/helios-commerce/helios/platform/httpx"
	"github.com/helios-commerce/helios/platform/redisx"
	"github.com/helios-commerce/helios/platform/shutdown"
)

func main() {
	ctx := context.Background()
	rt, err := boot.New(ctx, "gateway", false, true, false)
	if err != nil {
		slog.Error("boot", "err", err)
		os.Exit(1)
	}
	g := &gateway{rt: rt, routes: map[string]string{
		"/v1/auth":          env("IDENTITY_URL", "http://identity:8080"),
		"/v1/products":      env("CATALOG_URL", "http://catalog:8080"),
		"/v1/search":        env("SEARCH_URL", "http://search:8080"),
		"/v1/inventory":     env("INVENTORY_URL", "http://inventory:8080"),
		"/v1/orders":        env("ORDER_URL", "http://order:8080"),
		"/v1/payments":      env("PAYMENT_URL", "http://payment:8080"),
		"/v1/fulfillment":   env("FULFILLMENT_URL", "http://fulfillment:8080"),
		"/v1/checkout":      env("ORCHESTRATOR_URL", "http://orchestrator:8080"),
		"/v1/notifications": env("NOTIFICATION_URL", "http://notification:8080"),
	}}
	rt.Server.Router.Route("/v1", func(r httpx.Router) {
		r.Get("/meta", g.meta)
		r.HandleFunc("/*", g.proxy)
	})
	go func() {
		rt.Log.Info("listen", "addr", rt.Cfg.HTTPAddr)
		if err := rt.Server.HTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			rt.Log.Error("http", "err", err)
		}
	}()
	shutdown.Run(rt.Log, rt.Cfg.ShutdownWait,
		shutdown.Hook{Name: "http", Fn: func(ctx context.Context) error { return rt.Server.HTTP.Shutdown(ctx) }},
		shutdown.Hook{Name: "otel", Fn: rt.Obs.Shutdown},
	)
}

type gateway struct {
	rt     *boot.Runtime
	routes map[string]string
}

func (g *gateway) meta(w http.ResponseWriter, r *http.Request) {
	httpx.JSON(w, http.StatusOK, map[string]any{
		"service": "helios-gateway",
		"routes":  keys(g.routes),
	})
}

func (g *gateway) proxy(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
		return
	}
	allowed, _, err := redisx.TakeToken(r.Context(), g.rt.Redis, "rl:"+clientKey(r), 50, 100)
	if err != nil {
		g.rt.Log.Warn("ratelimit redis", "err", err)
	} else if !allowed {
		w.Header().Set("Retry-After", "1")
		httpx.Problem(w, http.StatusTooManyRequests, "rate limited")
		return
	}
	target := g.match(r.URL.Path)
	if target == "" {
		httpx.Problem(w, http.StatusNotFound, "unknown route")
		return
	}
	u, err := url.Parse(target)
	if err != nil {
		httpx.Problem(w, http.StatusBadGateway, "bad upstream")
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	proxy.FlushInterval = 50 * time.Millisecond
	orig := proxy.Director
	proxy.Director = func(req *http.Request) {
		orig(req)
		req.Host = u.Host
		req.Header.Set("X-Forwarded-Host", r.Host)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		g.rt.Log.Error("upstream", "err", err, "path", r.URL.Path)
		httpx.Problem(w, http.StatusBadGateway, "upstream unavailable")
	}
	proxy.ServeHTTP(w, r)
}

func (g *gateway) match(path string) string {
	best := ""
	found := ""
	for prefix, dest := range g.routes {
		if strings.HasPrefix(path, prefix) && len(prefix) > len(best) {
			best = prefix
			found = dest
		}
	}
	return found
}

func clientKey(r *http.Request) string {
	if ip := r.Header.Get("X-Forwarded-For"); ip != "" {
		return strings.Split(ip, ",")[0]
	}
	return r.RemoteAddr
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
