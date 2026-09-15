# Change data capture

## The problem Debezium actually solves

Dual-write is the bug: `INSERT order` then `producer.Send`. One of them will fail, and you will not know which, and you will either lose the event or invent a unique-constraint retry that nobody documented.

The outbox table makes the write **one commit**. Something still has to publish that row to Kafka.

## Path A — application relay (default locally for most services)

`platform/outbox.Relay`:

```
WITH cte AS (
  SELECT id FROM outbox
  WHERE published_at IS NULL AND next_attempt_at <= now()
  ORDER BY created_at
  LIMIT 50
  FOR UPDATE SKIP LOCKED
)
UPDATE outbox SET publish_tries = publish_tries + 1, ...
```

`SKIP LOCKED` is the whole trick: many relay goroutines (or many pods) can drain the same table without waiting on each other. Exponential `next_attempt_at` prevents a poison payload from spinning.

Lag is a first-class metric. If `outbox_unpublished_count` grows, Kafka or the producer is sick — not the business API.

Toggle with `OUTBOX_RELAY_ENABLED` (default `true`). Set `false` when Debezium owns that service's outbox.

## Path B — Debezium Outbox EventRouter (catalog + order on Helm minikube)

Connectors in `deployments/debezium/` read the **outbox table** via `pgoutput`, then Debezium's **Outbox EventRouter** SMT extracts the envelope `payload` and publishes to the same domain topics the Go relay uses:

| Connector | Database | Domain topic |
|---|---|---|
| `helios-catalog-outbox` | `catalog` | `helios.catalog.events` |
| `helios-order-outbox` | `orders` | `helios.order.events` |

That is intentional:

- Business tables have columns you do not want on a public topic (PII, internal versions).
- The outbox payload is already a versioned `events.Envelope` — Search / Notification keep the same consumer code.
- Ops can `UPDATE` a product in a pinch; if they do, **no event fires** unless they go through the API. That is a feature.

```
API ──tx──► products + outbox
                │
                ▼
         Postgres WAL (pgoutput)
                │
                ▼
     Debezium EventRouter ──► helios.catalog.events ──► Search → ES
```

On Helm (`values-minikube.yaml`):

- `debezium.enabled: true` deploys Kafka Connect + a post-upgrade job that registers connectors
- `catalog` / `order` set `OUTBOX_RELAY_ENABLED=false` so you do **not** double-publish
- Connect internal topics (`helios.connect.*`) are created with `cleanup.policy=compact`
- Connector heartbeats are disabled (`heartbeat.interval.ms=0`) so Connect does not need extra heartbeat topics when broker auto-create is off

Register manually (compose or port-forward):

```bash
make connectors
# CONNECT_URL=http://127.0.0.1:8083 make connectors
```

Postgres runs with `wal_level=logical` and enough replication slots. Migrations create publications `helios_catalog_outbox` / `helios_order_outbox`.

### Why not CDC `public.products`?

You *can* add a second connector on the business table. Then every DBA hotfix becomes an event, column renames leak into the public contract, and you still need a shaping step. Helios keeps the outbox as the contract boundary.

### Lag and `published_at`

With Debezium, inserts are captured from the WAL — the connector does not flip `published_at`. That column and `helios_*_outbox_unpublished_count` belong to the **relay** path. For Debezium, watch Connect connector status and replication slot lag instead.

## Search index

Search is a **projection**:

```
helios.catalog.events  ──consumer group search-indexer──►  Elasticsearch helios-products
```

The document id is `product_id`. Deletes / `active=false` remove the doc. Queries always filter `tenant_id` + `active`.

If ES is down, the consumer retries then DLQs (`helios.catalog.events.dlq`). A replay from the DLQ (or a timestamp reset on the consumer group) rebuilds the index because indexing is idempotent.

## Schema evolution

Event types are **dot-namespaced and version-suffixed**: `order.created.v1`. A breaking change is `v2` and a new consumer branch, not a silent field rename. The proto in `proto/events.proto` is the schema of record; the wire format is JSON so DLQs are operable with `jq` at 3am. Moving to Avro + Schema Registry is a mechanical swap of the kafka codec, not a redesign.

## Interview one-liner

> We use a transactional outbox for atomicity. Inventory and friends still use a Go poller locally. Catalog and order on minikube use Debezium's outbox EventRouter so WAL → Kafka with no dual-write and no Kafka inside the SQL transaction.
