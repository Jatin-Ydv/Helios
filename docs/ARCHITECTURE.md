# Helios architecture

Helios is a **multi-tenant commerce control plane**: catalog, inventory, checkout, payment, fulfillment, search, and notification as independently deployable Go services. It is deliberately not a CRUD tutorial. The interesting parts are the **failure domains**, **consistency model**, and **operability**.

## Why these service cuts

Service boundaries follow *write contention* and *failure isolation*, not noun-on-a-whiteboard.

| Service | Why it is separate |
|---|---|
| **Identity** | Auth is a different SLO and a different threat model than checkout. Compromising catalog must not dump password hashes. |
| **Catalog** | Read-heavy, cacheable, CQRS-friendly. Product writes are rare compared to search reads. Product **record** cache is Aerospike; Redis is not used as a shared blob cache for catalog. |
| **Inventory** | Hot row contention on `(tenant, warehouse, sku)`. Needs locks, fences, reservation TTLs. Must not share a connection pool with catalog browsers. |
| **Order** | Source of truth for the order aggregate and its state machine. |
| **Payment** | Circuit-broken dependency on a PSP. Bulkheaded so PSP latency cannot exhaust order workers. PCI-ish data never leaks into order JSON. |
| **Fulfillment** | Asynchronous, retry-friendly. A warehouse outage must not fail authorization. |
| **Orchestrator** | Owns the *checkout saga*, not the data. Calls Order/Inventory/Payment/Fulfillment over **gRPC**; exposes checkout over **HTTP** for the gateway. If it dies mid-flight, a timeout worker compensates. |
| **Search** | Derived read model. Elasticsearch is allowed to lag; Postgres is not. |
| **Gateway** | Edge policy: JWT, rate-limit, routing. Not a place for business rules. |

A "senior" mistake is putting reserve + charge + insert-order in one process "for simplicity". That process then has three independent outage modes and no compensation story.

## Consistency model

Helios is **not** distributed-ACID. It is:

1. **Strong consistency inside a service** — one Postgres primary per bounded context, row-level locks / optimistic versions.
2. **Atomicity of "state change + domain event"** — transactional outbox. The business row and the `outbox` row commit together. Kafka is *not* in the SQL transaction (it cannot be).
3. **At-least-once delivery between services** — Kafka + inbox unique `(event_id)`. Consumers are idempotent.
4. **Bounded staleness for search** — catalog outbox (or Debezium CDC of that outbox) → search indexer → ES. Search is never the write path.

If a candidate says "we use Kafka for consistency", they are wrong. Kafka is a **transport**. Consistency is in the outbox, the inbox, and the saga.

## Dual publish path: relay vs Debezium

Two ways to drain `outbox`, both valid, both in this repo:

```
Postgres commit
    │
    ├─► [dev] Go relay  SELECT … FOR UPDATE SKIP LOCKED  ──► Kafka
    │
    └─► [prod] Debezium pgoutput on table `outbox`
            WAL ──► Connect ──► Kafka
```

The Go relay is an **application-level poller**. It is easy to run, easy to debug, and has a visible lag metric (`helios_*_outbox_unpublished_count`).

Debezium is the **production preference** at scale: no polling interval, no extra DB load proportional to tick rate, and it survives a total replay from WAL if you keep slots. The connector JSON lives in `deployments/debezium/`. Postgres is started with `wal_level=logical`.

You do **not** run both against the same rows in production without a published-at filter — you would double-publish. Pick one. Helios defaults to the relay locally so `docker compose up` does not depend on Connect being healthy.

## Partitioning and ordering

Producers use `aggregate_id` as the Kafka key (hash balancer). That gives **per-aggregate ordering** (all events for order `01H…` land on one partition) without pretending the log is totally ordered. Search can consume catalog events with a different consumer group and lag independently.

Topics are created with 6 partitions locally; production sizing is `target_throughput / (partition_max_bytes/s)` plus headroom for rebalances, not "one partition per microservice".

## Identity of a request

Every request carries:

- `X-Correlation-ID` (ULID, generated at the gateway if missing)
- OpenTelemetry `traceparent` (W3C), injected into Kafka headers and the event envelope
- JWT `tid` / `uid` / `role`, copied onto `tenant.Identity` in context
- Optional `Idempotency-Key` for POSTs that must not double-charge

Causation vs correlation: correlation is the customer-facing request; causation is the event that triggered this one. They are how you answer "why did this compensation fire" at 3am.

## What we explicitly do not do

- **Two-phase commit across services.** XA with Kafka + Postgres + PSP is a career-limiting move.
- **Shared database.** Cross-service joins are a distributed monolith.
- **Floats for money.** `platform/money` is integer minor units.
- **Cache-as-source-of-truth.** Aerospike caches product documents with TTL; Redis is a lock, a rate limiter, and a session hint. Inventory counts live in Postgres. See [`docs/AEROSPIKE.md`](AEROSPIKE.md).
- **Exactly-once Kafka.** Kafka's EOS is broker-side. Helios treats EOS as "idempotent handler + inbox unique constraint", which survives producer retries and consumer rebalances.

## Multi-tenancy

Logical tenancy: `tenant_id` on every row and every event. Gateway rejects requests without a tenant (JWT or internal header). This is the model Shopify-scale systems start with. Cell-based / DB-per-tenant is a *deployment* overlay: the same queries already predicate on `tenant_id`, so you can later pin a tenant to a shard without rewriting handlers.

## Failure injection

Payment talks to a simulated PSP with `PSP_FAILURE_RATE_PCT` and `PSP_LATENCY_MS`. Combined with the circuit breaker and bulkhead, you can watch checkout compensate in Jaeger without mocking at the HTTP layer.
