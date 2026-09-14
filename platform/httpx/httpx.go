package httpx

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/helios-commerce/helios/platform/auth"
	"github.com/helios-commerce/helios/platform/events"
	"github.com/helios-commerce/helios/platform/health"
	"github.com/helios-commerce/helios/platform/id"
	"github.com/helios-commerce/helios/platform/metrics"
	"github.com/helios-commerce/helios/platform/observability"
	"github.com/helios-commerce/helios/platform/tenant"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

type Router = chi.Router

type Server struct {
	Router *chi.Mux
	HTTP   *http.Server
}

func NewServer(addr string, h *observability.Handle, m *metrics.ServiceMetrics, hc *health.Checker) *Server {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(correlation)
	r.Use(metricsMW(m))
	r.Use(accessLog(h))
	r.Get("/healthz", hc.Live())
	r.Get("/readyz", hc.Ready())
	r.Handle("/metrics", h.MetricsHandler())

	handler := otelhttp.NewHandler(r, "http.server", otelhttp.WithSpanNameFormatter(func(_ string, req *http.Request) string {
		return req.Method + " " + req.URL.Path
	}))
	return &Server{
		Router: r,
		HTTP: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

func correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cid := r.Header.Get("X-Correlation-ID")
		if cid == "" {
			cid = id.NewULID()
		}
		w.Header().Set("X-Correlation-ID", cid)
		ctx := events.WithCorrelation(r.Context(), cid)
		if idem := r.Header.Get("Idempotency-Key"); idem != "" {
			ctx = events.WithIdempotency(ctx, idem)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func metricsMW(m *metrics.ServiceMetrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if m == nil {
				next.ServeHTTP(w, r)
				return
			}
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			route := r.URL.Path
			m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(ww.Status())).Inc()
			m.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		})
	}
}

func accessLog(h *observability.Handle) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/health") || r.URL.Path == "/metrics" || r.URL.Path == "/readyz" || r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			log := observability.LoggerWithSpan(r.Context(), h.Logger)
			log.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"ms", time.Since(start).Milliseconds(),
				"correlation_id", events.CorrelationID(r.Context()),
			)
		})
	}
}

func Auth(jwt *auth.Service) func(http.Handler) http.Handler {
	return AuthFlexible(jwt, "")
}

// AuthFlexible accepts a user JWT or a mesh-internal token plus tenant headers.
// The internal path is how the saga orchestrator calls downstreams without
// impersonating a customer's access token lifetime.
func AuthFlexible(jwt *auth.Service, internalToken string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if internalToken != "" && r.Header.Get("X-Helios-Internal") == internalToken {
				tid := r.Header.Get("X-Tenant-ID")
				if tid == "" {
					JSON(w, http.StatusUnauthorized, Map{"error": "internal call missing tenant"})
					return
				}
				ctx := tenant.With(r.Context(), tenant.Identity{
					TenantID: tid,
					UserID:   headerOr(r, "X-User-ID", "service"),
					Role:     "service",
				})
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			h := r.Header.Get("Authorization")
			if !strings.HasPrefix(h, "Bearer ") {
				JSON(w, http.StatusUnauthorized, Map{"error": "missing bearer token"})
				return
			}
			claims, err := jwt.Parse(strings.TrimPrefix(h, "Bearer "))
			if err != nil {
				JSON(w, http.StatusUnauthorized, Map{"error": "invalid token"})
				return
			}
			ctx := auth.ContextFromClaims(r.Context(), claims)
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(observability.SpanAttrTenant(claims.TenantID))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func headerOr(r *http.Request, k, def string) string {
	if v := r.Header.Get(k); v != "" {
		return v
	}
	return def
}

func RequireRole(roles ...string) func(http.Handler) http.Handler {
	set := map[string]struct{}{}
	for _, r := range roles {
		set[r] = struct{}{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ident, err := tenant.From(r.Context())
			if err != nil {
				JSON(w, http.StatusUnauthorized, Map{"error": "unauthenticated"})
				return
			}
			if _, ok := set[ident.Role]; !ok && ident.Role != "admin" {
				JSON(w, http.StatusForbidden, Map{"error": "forbidden"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type Map map[string]any

func JSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func Decode(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func Problem(w http.ResponseWriter, code int, msg string) {
	JSON(w, code, Map{"error": msg, "code": http.StatusText(code)})
}

type Client struct {
	http *http.Client
	base string
}

func NewClient(base string, timeout time.Duration) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{
			Timeout: timeout,
			Transport: otelhttp.NewTransport(&http.Transport{
				MaxIdleConnsPerHost: 32,
			}),
		},
	}
}

func (c *Client) DoJSON(ctx context.Context, method, path string, headers map[string]string, in, out any) (int, error) {
	var body *strings.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = strings.NewReader(string(b))
	} else {
		body = strings.NewReader("")
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cid := events.CorrelationID(ctx); cid != "" {
		req.Header.Set("X-Correlation-ID", cid)
	}
	if ident, err := tenant.From(ctx); err == nil {
		req.Header.Set("X-Tenant-ID", ident.TenantID)
		req.Header.Set("X-User-ID", ident.UserID)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}
