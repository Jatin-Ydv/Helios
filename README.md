# Helios

**A production-shaped, multi-tenant commerce platform in Go.**  
Catalog, inventory, checkout saga, payments, fulfillment, search (Elasticsearch), CDC (Debezium), Kafka, Redis, Aerospike (product cache), and distributed tracing — designed to be read by a principal engineer, not a bootcamp rubric.

Helios exists to demonstrate *how* a senior engineer cuts services, chooses consistency, and operates failure — not to reinvent Shopify.

| Talk to an interviewer about | Where it lives |
|---|---|
| Transactional outbox vs dual-write | `platform/outbox`, `docs/CDC.md` |
| Debezium on the outbox (WAL, not poll) | `deployments/debezium/` |
| Orchestrated checkout saga + timeouts | `services/orchestrator`, `docs/SAGAS.md` |
| gRPC for saga internals (HTTP at the edge) | `proto/helios/v1`, `docs/GRPC.md` |
| Inventory reservation, Redis fence, TTL sweeper | `services/inventory` |
| Circuit breaker + bulkhead around a PSP | `platform/resilience`, `services/payment` |
| Idempotent consumers (inbox) and DLQ | `platform/inbox`, `platform/kafka` |
| W3C trace context over HTTP **and** Kafka | `platform/events`, `platform/observability` |
| Money as integer minor units | `platform/money` |
| Edge rate-limit (token bucket in Redis) | `platform/redisx`, `services/gateway` |
| Catalog product record cache in Aerospike | `platform/aerospikex`, `docs/AEROSPIKE.md` |
| Helm packaging (values per environment) | `deployments/helm/helios`, `docs/HELM.md` |
| CQRS search projection into Elasticsearch | `services/search` |

Architecture deep-dives: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/SAGAS.md`](docs/SAGAS.md) · [`docs/CDC.md`](docs/CDC.md) · [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) · [`docs/AEROSPIKE.md`](docs/AEROSPIKE.md) · [`docs/GRPC.md`](docs/GRPC.md) · [`docs/HELM.md`](docs/HELM.md)

---

## Topology

```
                         ┌──────────────┐
          JWT + RL       │    Gateway   │
                         └──────┬───────┘
                                │
        ┌──────────┬────────────┼────────────┬──────────┐
        ▼          ▼            ▼            ▼          ▼
   Identity    Catalog     Orchestrator    Search   Notification
                   │            │
                   │            ├─ Order
        Aerospike  │            ├─ Inventory ── Postgres (row lock + version)
        product    │            ├─ Payment ──── circuit breaker ── PSP
        cache      │            └─ Fulfillment
                   │
        Redis ─────┼── locks, fence, rate-limit, session hint
                   │
                   ▼
              outbox ──► Kafka ──► Search indexer ──► Elasticsearch
                   ▲
                   └── Debezium (optional, wal_level=logical)

Jaeger ◄── OTLP/HTTP from every process
Prometheus ── scrape /metrics
```

Ten services, **database per bounded context**, one Kafka cluster, one Redis, one Aerospike (catalog cache), one ES.

---

## Quick start

You want **8+ GB RAM**. ES + Kafka are not toys. On a laptop, prefer minikube (below) over Compose with all sidecar UIs.

### Kubernetes (minikube) — recommended to run locally

Images are built **inside minikube's Docker daemon** so kubelet never tries to pull `helios/*` from a registry. One replica each, small heaps.

```bash
make minikube-up     # start cluster, build 10 images, apply kustomize overlay
make helm-up         # alternatively: Helm install/upgrade with values-minikube.yaml
make minikube-demo   # same checkout flow through the NodePort gateway
make minikube-down   # minikube delete
make helm-down       # uninstall Helm release only
```
| Surface | How to open |
|---|---|
| Gateway | `kubectl -n helios port-forward svc/gateway 8080:8080` then http://localhost:8080 |
| Jaeger | `kubectl -n helios port-forward svc/jaeger 16686:16686` then http://localhost:16686 |

Docker Desktop + WSL often cannot hit `$(minikube ip):30080` directly; port-forward is the reliable path. Native Linux Docker Engine can use the NodePorts (gateway `30080`, Jaeger `30686`).

Manifests: `deployments/k8s/minikube/` (Postgres, Redis, Kafka, ES, Jaeger + all 10 services). The production-shaped Deployments/HPA/PDBs live under `deployments/k8s/base/`.

### Docker Compose (full sidecar stack)

```bash
make up          # infra + all services
make connectors  # optional: register Debezium outbox connectors
make demo        # register tenant, catalog SKU, stock it, checkout
```

| Surface | URL |
|---|---|
| Gateway | http://localhost:8080 |
| Jaeger | http://localhost:16686 |
| Grafana (admin/admin) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Kafka UI | http://localhost:8081 |
| Kafka Connect | http://localhost:8083 |
| Elasticsearch | http://localhost:9200 |

```bash
make test        # unit tests for money, breakers, envelopes
make down        # destroy volumes too
```

Run a single service against compose infra:

```bash
export POSTGRES_DSN='postgres://helios:helios@localhost:5432/order?sslmode=disable'
export KAFKA_BROKERS=localhost:9094
export REDIS_ADDR=localhost:6379
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318
go run ./services/order/cmd/order
```

---

## Request path (checkout)

1. `POST /v1/auth/register` — bcrypt, JWT, refresh rotation in Postgres, session hint in Redis.
2. `POST /v1/products` — writes `products` **and** `outbox` in one transaction; Aerospike product cache updated (invalidated on update/delete).
3. `POST /v1/inventory/stock` — `on_hand` for `(tenant, warehouse, sku)`.
4. `POST /v1/checkout` — orchestrator:
   - create `pending` order (idempotency key)
   - reserve qty (Redis lock + `SELECT FOR UPDATE` + fencing token + 15m TTL)
   - authorize payment (retries, bulkhead, breaker)
   - confirm order, allocate shipment
   - on any failure: compensate (release, void, fail order)
5. Async: outbox relay (or Debezium) publishes to Kafka; search indexes the product; notification records the confirmation.

Open the checkout trace in Jaeger. You should see one trace covering four downstream HTTP calls plus Kafka produce spans.

---

## Design rules this repo refuses to break

1. **No distributed transactions.** Sagas and outbox only.
2. **No floats in money.** `int64` minor units + ISO currency.
3. **No business logic in the gateway.** Routing, auth, rate limits.
4. **No cache as source of truth for stock.** Postgres is the ledger; Redis is the mutex; Aerospike is the product read cache only.
5. **No silent event versioning.** `order.created.v1` is a contract. Breaking changes are `v2`.
6. **No liveness probe that pings Kafka.** Liveness = process. Readiness = dependencies.
7. **Graceful shutdown is LIFO:** stop ready → drain HTTP → close consumers → flush traces.

---

## Repository layout

```
platform/           reusable infrastructure (the actual senior work)
  outbox/ inbox/ kafka/ resilience/ observability/ redisx/ aerospikex/ auth/ …
services/           one binary per bounded context, hexagonal-ish
  */cmd/*/main.go   composition root
  */migrations/     SQL applied at boot
deployments/
  docker/           Dockerfile, topic + database init
  debezium/         Kafka Connect connector JSON
  observability/    Prometheus, Grafana, alert rules
  k8s/base/         Deployments, HPA, PDBs, kustomize
proto/              schema of record for the event envelope
docs/               the decisions, not the generated boilerplate
scripts/demo.sh     one command that exercises the saga
```

---

## Interview notes (steal these)

- **Why outbox on the outbox table for Debezium, not `products`?** Because the envelope is already the public contract; WAL of a business table leaks columns and skips domain invariants.
- **Why a fencing token on the inventory lock?** Kleppmann: a lock TTL can expire while the holder is paused (GC). The fence is stored on the reservation row; a zombie write with a lower fence is ignored.
- **Why orchestrate checkout but choreograph notifications?** Because compensation graphs need an owner; fan-out does not.
- **Why 10% trace sampling in prod?** Cost. Tail-based sampling belongs in the collector, keyed on checkout errors, not on every catalog GET.
- **What is the availability math?** Payment is the weakest dependency. Bulkhead (16 in-flight) + breaker (5 failures / 8s) isolate it. Checkout degrades to `409 compensated`, not to a thread explosion.

This is the system I want to be on-call for. That is the bar.
