# Change data capture

## The problem Debezium actually solves

Dual-write is the bug: `INSERT order` then `producer.Send`. One of them will fail, and you will not know which, and you will either lose the event or invent a unique-constraint retry that nobody documented.

The outbox table makes the write **one commit**. Something still has to publish that row to Kafka.

## Path A — application relay (default locally)

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

## Path B — Debezium (production)

Connectors in `deployments/debezium/` read the **outbox table** via `pgoutput`, not the business tables. That is intentional:

- Business tables have columns you do not want on a public topic (PII, internal versions).
- The outbox payload is already a versioned `events.Envelope`.
- Ops can `UPDATE` a product in a pinch; if they do, **no event fires** unless they go through the API. That is a feature. If you *want* log-tailing of `products` itself (catching out-of-band SQL), add a second connector on `public.products` and accept that you now have two schemas to reconcile.

Register with:

```bash
make connectors
```

Postgres is started with `wal_level=logical`. The catalog migration attempts `CREATE PUBLICATION helios_catalog_outbox FOR TABLE outbox`.

## Search index

Search is a **projection**:

```
helios.catalog.events  ──consumer group search-indexer──►  Elasticsearch helios-products
```

The document id is `product_id`. Deletes / `active=false` remove the doc. Queries always filter `tenant_id` + `active`.

If ES is down, the consumer retries then DLQs (`helios.catalog.events.dlq`). A replay from the DLQ (or a timestamp reset on the consumer group) rebuilds the index because indexing is idempotent.

## Schema evolution

Event types are **dot-namespaced and version-suffixed**: `order.created.v1`. A breaking change is `v2` and a new consumer branch, not a silent field rename. The proto in `proto/events.proto` is the schema of record; the wire format is JSON so DLQs are operable with `jq` at 3am. Moving to Avro + Schema Registry is a mechanical swap of the kafka codec, not a redesign.
