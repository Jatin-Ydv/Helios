# Observability

Three pillars, one correlation key. If a service logs without a `trace_id`, it is unfinished.

## Traces

OpenTelemetry SDK, OTLP/HTTP to Jaeger (`localhost:16686` locally).

Propagation:

- HTTP: W3C `traceparent` via `otelhttp`
- Kafka: the same fields copied onto message headers **and** into `Envelope.Trace` so a consumer that only sees the body can still join the trace
- Saga: one trace from `POST /v1/checkout` through order, inventory, payment, fulfillment

Sampler: 100% in `local`, 10% parent-based in `prod`. Tune with tail sampling at the collector in a real cell, not in the app.

## Metrics

Each process exposes `/metrics` (Prometheus). Cardinality rules:

- Route labels are paths, not raw URLs with IDs (gateway still uses path; high-cardinality IDs are forbidden)
- Circuit breaker state is a gauge `0|1|2`
- Outbox lag is a gauge, scraped every 10s — this is the SLO proxy for "events are flowing"

Alert rules live in `deployments/observability/alerts.yml`. They encode **user-facing SLOs**, not "CPU > 80%".

## Logs

`slog` JSON to stdout. `observability.LoggerWithSpan` adds `trace_id` and `span_id`. Correlation id is on every access log line.

In production this goes to a collector (Loki/ELK). Helios does not ship a custom log agent; that is cluster infrastructure.

## Health

- `/healthz` — process is alive (kube liveness). No dependency checks. A false-down liveness probe kills a pod that is still draining.
- `/readyz` — Postgres/Redis/ES pings. Kube readiness removes the pod from Service endpoints. The shutdown hook flips ready off *before* the HTTP server stops accepting, so we stop getting traffic then drain.

## What to look at after `make demo`

1. Grafana `Helios — Commerce Platform` (port 3000)
2. Jaeger: service `orchestrator`, operation `POST /v1/checkout`
3. Kafka UI (port 8081): `helios.order.events` should show `order.created.v1` then `order.confirmed.v1`
4. `curl localhost:8103/metrics` (order) for `helios_order_outbox_unpublished_count`
