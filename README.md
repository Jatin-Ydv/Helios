# Helios

**A multi-tenant commerce platform in Go** — catalog, inventory, checkout saga, payments, fulfillment, search, and notifications.

Helios is a production-*shaped* reference system: service boundaries, consistency choices, and failure handling over a toy CRUD demo.

| Area | Where it lives |
|---|---|
| Transactional outbox vs dual-write | `platform/outbox`, [`docs/CDC.md`](docs/CDC.md) |
| Debezium outbox EventRouter (catalog/order) | `deployments/debezium/`, [`docs/CDC.md`](docs/CDC.md) |
| Orchestrated checkout saga + compensation | `services/orchestrator`, [`docs/SAGAS.md`](docs/SAGAS.md) |
| gRPC for saga internals (HTTP at the edge) | `proto/helios/v1`, [`docs/GRPC.md`](docs/GRPC.md) |
| Inventory reservation, Redis fence, TTL sweeper | `services/inventory` |
| Circuit breaker + bulkhead around a PSP | `platform/resilience`, `services/payment` |
| Idempotent consumers (inbox) and DLQ | `platform/inbox`, `platform/kafka` |
| W3C trace context over HTTP and Kafka | `platform/events`, `platform/observability` |
| Money as integer minor units | `platform/money` |
| Edge rate limit (token bucket in Redis) | `platform/redisx`, `services/gateway` |
| Catalog product record cache (Aerospike) | `platform/aerospikex`, [`docs/AEROSPIKE.md`](docs/AEROSPIKE.md) |
| Helm packaging | `deployments/helm/helios`, [`docs/HELM.md`](docs/HELM.md) |
| CQRS search projection (Elasticsearch) | `services/search` |

Deep dives: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/SAGAS.md`](docs/SAGAS.md) · [`docs/CDC.md`](docs/CDC.md) · [`docs/OBSERVABILITY.md`](docs/OBSERVABILITY.md) · [`docs/AEROSPIKE.md`](docs/AEROSPIKE.md) · [`docs/GRPC.md`](docs/GRPC.md) · [`docs/HELM.md`](docs/HELM.md)

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
                   │            ├─ Order        (gRPC from orchestrator)
        Aerospike  │            ├─ Inventory
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

Ten services, **database per bounded context**, shared Kafka, Redis, Aerospike (catalog cache), and Elasticsearch.

---

## Quick start

Prefer **8+ GB RAM**. Elasticsearch and Kafka are heavy; on a laptop, minikube is usually better than full Compose with every UI sidecar.

### Kubernetes (minikube) — recommended

```bash
make minikube-up     # start cluster, build images, apply kustomize overlay
make helm-up         # or: Helm install/upgrade with values-minikube.yaml
make minikube-demo   # end-to-end checkout through the gateway
make minikube-down   # delete the minikube cluster
make helm-down       # uninstall the Helm release only
```

| Surface | How to open |
|---|---|
| Gateway | `kubectl -n helios port-forward svc/gateway 8080:8080` → http://localhost:8080 |
| Jaeger | `kubectl -n helios port-forward svc/jaeger 16686:16686` → http://localhost:16686 |

On some Docker Desktop / WSL setups, NodePorts on `$(minikube ip)` are unreachable; port-forward is the reliable path. Native Linux Docker Engine can use NodePorts (gateway `30080`, Jaeger `30686`).

Manifests: `deployments/k8s/minikube/`. Production-shaped Deployments / HPA / PDBs: `deployments/k8s/base/`. Helm chart: `deployments/helm/helios/`.

### Docker Compose

```bash
make up          # infra + all services
make connectors  # register/update Debezium outbox connectors (Compose or port-forward)
make demo        # register tenant → product → stock → checkout
make test
make down        # tear down (including volumes)
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

Run one service against Compose infra:

```bash
export POSTGRES_DSN='postgres://helios:helios@localhost:5432/orders?sslmode=disable'
export KAFKA_BROKERS=localhost:9094
export REDIS_ADDR=localhost:6379
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4318
go run ./services/order/cmd/order
```

---

## Request path (checkout)

1. `POST /v1/auth/register` — bcrypt, JWT, refresh rotation in Postgres; session hint in Redis.
2. `POST /v1/products` — writes `products` and `outbox` in one transaction; Aerospike cache updated (invalidated on update/delete).
3. `POST /v1/inventory/stock` — set `on_hand` for `(tenant, warehouse, sku)`.
4. `POST /v1/checkout` — orchestrator saga over **gRPC**:
   - create `pending` order (idempotency key)
   - reserve qty (Redis lock + `SELECT FOR UPDATE` + fencing token + 15m TTL)
   - authorize payment (retries, bulkhead, breaker)
   - confirm order, allocate shipment
   - on failure: compensate (release, void, fail order)
5. Async: Debezium (catalog/order) or outbox relay → Kafka → search indexes the product; notification records confirmation.

In Jaeger, one checkout trace should span orchestrator → order / inventory / payment / fulfillment plus Kafka produce spans.

---

## Design rules

1. **No distributed transactions** across services — sagas and outbox only.
2. **No floats for money** — `int64` minor units + ISO currency.
3. **No business logic in the gateway** — routing, auth, rate limits.
4. **No cache as source of truth for stock** — Postgres is the ledger; Redis is the mutex; Aerospike is the product read cache only.
5. **No silent event versioning** — `order.created.v1` is a contract; breaking changes are `v2`.
6. **Liveness ≠ readiness** — liveness is process health; readiness checks dependencies.
7. **Graceful shutdown is LIFO** — stop ready → drain HTTP → close consumers → flush traces.

---

## Repository layout

```
platform/           shared libraries (outbox, kafka, resilience, aerospikex, …)
services/           one binary per bounded context
  */cmd/*/main.go   composition root
  */migrations/     SQL applied at boot
deployments/
  docker/           Dockerfile, topic + database init
  debezium/         Kafka Connect connector JSON
  observability/    Prometheus, Grafana, alert rules
  k8s/              Kustomize base + minikube overlay
  helm/helios/      Helm chart + values-minikube.yaml
proto/              event envelope + checkout gRPC contracts
gen/                generated protobuf stubs
docs/               architecture decisions
scripts/demo.sh     one-command saga exercise
```

---

## Design rationale (selected)

- **Outbox table (not raw `products`) for CDC** — the envelope is the public contract; WAL of a business table leaks columns and skips domain shaping.
- **Fencing token on inventory locks** — lock TTLs can expire while a holder is paused (e.g. GC); the fence on the reservation row rejects zombie writes.
- **Orchestrate checkout, choreograph notifications** — compensation needs an owner; fan-out does not.
- **Payment isolation** — bulkhead + circuit breaker keep PSP latency from exhausting the mesh; failed checkout returns compensated state instead of cascading load.
