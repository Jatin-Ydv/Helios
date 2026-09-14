// Package observability boots the three pillars as one unit: structured logs
// with trace IDs, OTLP traces, and Prometheus metrics. A service that does not
// call Setup is considered incomplete.
package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type Handle struct {
	Logger   *slog.Logger
	Tracer   trace.Tracer
	Registry *prometheus.Registry
	tp       *sdktrace.TracerProvider
}

func Setup(ctx context.Context, service, env, otlpEndpoint, level string) (*Handle, error) {
	logger := newLogger(service, env, level)
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(service),
			semconv.ServiceNamespace("helios"),
			semconv.DeploymentEnvironment(env),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(normalizeOTLP(otlpEndpoint)),
	}
	if env != "prod" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		logger.Warn("otlp exporter unavailable; traces will be no-op", "err", err)
		exp = nil
	}

	var tp *sdktrace.TracerProvider
	if exp != nil {
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio(env)))),
		)
	} else {
		tp = sdktrace.NewTracerProvider(sdktrace.WithResource(res))
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &Handle{
		Logger:   logger,
		Tracer:   otel.Tracer(service),
		Registry: reg,
		tp:       tp,
	}, nil
}

func (h *Handle) Shutdown(ctx context.Context) error {
	if h.tp != nil {
		return h.tp.Shutdown(ctx)
	}
	return nil
}

func (h *Handle) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(h.Registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func LoggerWithSpan(ctx context.Context, log *slog.Logger) *slog.Logger {
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return log
	}
	return log.With(
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	)
}

func SpanAttrTenant(tenantID string) attribute.KeyValue {
	return attribute.String("helios.tenant_id", tenantID)
}

func newLogger(service, env, level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("service", service, "env", env)
}

func normalizeOTLP(ep string) string {
	ep = strings.TrimPrefix(ep, "http://")
	ep = strings.TrimPrefix(ep, "https://")
	return ep
}

func sampleRatio(env string) float64 {
	if env == "prod" {
		return 0.1
	}
	return 1.0
}

// Discard is used in tests.
func Discard() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
