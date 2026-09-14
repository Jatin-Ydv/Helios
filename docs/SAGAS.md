# Checkout saga

Checkout is an **orchestrated saga**, not a single SQL transaction and not a purely choreographed hopscotch of events.

## Why orchestration here

Choreography ("inventory reserved → payment listens → payment authorized → order listens") looks elegant until:

- you need a timeout that spans three services
- you need to see *where* a checkout is stuck
- compensation order matters (void before release is a finance preference; both must happen)

Orchestration puts the state machine in one place (`sagas` + `saga_steps`) with a **timeout worker** that compensates in-flight instances. Choreography is still used *after* checkout: `order.confirmed` fans out to notification and analytics without the orchestrator caring.

## Happy path

```
Client
  │  POST /v1/checkout
  ▼
Orchestrator                 (state: started)
  │  POST /v1/orders                  ─► Order   (pending, outbox OrderCreated)
  │  POST /v1/inventory/reserve  × N  ─► Inventory (reserved, TTL 15m)
  │  POST /v1/payments/authorize      ─► Payment  (PSP + breaker + bulkhead)
  │  POST /v1/orders/{id}/confirm     ─► Order   (confirmed)
  │  POST /v1/fulfillment/allocate    ─► Fulfillment (retryable; not compensated)
  ▼
state: completed
  + NotificationRequested via outbox
```

Authorize-then-capture: the PSP authorization is a hold. Capture is a separate call (`POST /v1/payments/{id}/capture`) intended to run at ship time. This matches card-network economics better than capture-on-click.

## Compensations

| Failed step | Compensation |
|---|---|
| Order create | nothing to undo |
| Reserve | `orders/{id}/fail` |
| Authorize | release every SKU + fail order |
| Confirm | void payment + release SKUs + fail order |
| Allocate | **no compensation** — fulfillment is retried; money and stock are already correct |

This last row is the Amazon-shaped decision: you do not un-charge a customer because a warehouse WMS blipped. You retry allocation.

## Timeouts

Every saga row has `expires_at` (~45s locally). A loop in the orchestrator:

1. selects expired non-terminal sagas
2. runs the same compensation path
3. marks `compensated` with reason `saga timeout`

Without this, a crash after reserve and before authorize leaks stock until the inventory TTL (15 minutes) fires. The TTL is a **safety net**, not the primary compensator.

## Idempotency

- Client sends `Idempotency-Key` on checkout (gateway forwards).
- Order create persists the key in `idempotency_keys` with a request hash. Same key + same body replays the response. Same key + different body is `422`.
- Inventory reservations are unique on `(tenant, order_id, sku)` so a retried reserve is a no-op insert.
- Payments are unique on `(tenant, order_id)`.

## State storage

Saga state is Postgres, not Kafka. Kafka is a terrible workflow engine: you cannot query "all sagas in `payment_authorized` older than 30s" without a compacted changelog and a custom store — which is what you just rebuilt, worse.

## Compared to Temporal / Cadence

A real production team at scale should consider Temporal for the orchestrator *process*. Helios implements the saga in-process so the **pattern is visible in a single repo** — the state machine, the compensations, the timeout, the metrics — without a second runtime. The service boundary is such that swapping the orchestrator internals for a Temporal worker is a local change.
