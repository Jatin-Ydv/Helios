# Aerospike in Helios

Helios uses **polyglot persistence by access pattern**. Aerospike is the catalog **product record cache**. It is not a second source of truth for products, stock, or orders.

## Interview one-liner

> Helios stores durable domain state in Postgres, coordinates ephemeral work in Redis (locks, fencing, rate limits), fans out events on Kafka, projects search into Elasticsearch, and caches catalog product **records** in Aerospike so the hot read path does not share a Redis process with inventory mutexes and gateway token buckets.

## Why Aerospike here (not Redis)

| Workload | Store | Why |
|---|---|---|
| Product `GET` by id (TTL ~5m) | **Aerospike** | Primary-key **record** cache: namespace `helios`, set `products`, bin `json`. Read-heavy catalog traffic. |
| Inventory lock + fence | **Redis** | Short-lived mutex + INCR fence; Lua compare-and-del. |
| Gateway rate limit | **Redis** | Token bucket / counters. |
| Identity session hint | **Redis** | Tiny ephemeral key. |
| Product / order / stock truth | **Postgres** | Durability and transactions. |
| Full-text search | **Elasticsearch** | Derived index from Kafka, not the write path. |

**Workload isolation:** if Redis is under pressure from locks and rate limits, catalog browser traffic should not compete for the same `maxmemory` and eviction policy. Aerospike owns the product cache hot path.

**Record model:** Aerospike keys map cleanly to `(tenant, product)`. Bins allow evolving the cached shape (today one `json` bin; later field-level bins) without treating Redis as a blob store and a lock manager at once.

**What not to claim:** “Aerospike is faster than Redis” without a workload. The decision is **access-pattern fit** and **failure-domain separation**, not a generic speed contest. Local minikube runs a **single-node** CE instance (`replication-factor 1`, memory storage `data-size` ≥ 256M); production would be a multi-node cluster.

## Data model

- Namespace: `helios` (`AEROSPIKE_NAMESPACE`)
- Set: `products`
- User key: `{tenant_id}:{product_id}`
- Bin: `json` — marshaled product document
- TTL: 5 minutes on write (client write policy)

Postgres remains authoritative. Cache miss → load from Postgres → `Put`. Update/delete → invalidate (`Delete`).

## Code map

| Piece | Path |
|---|---|
| Client + cache | `platform/aerospikex` |
| Catalog wiring | `services/catalog/cmd/catalog/main.go` |
| Config | `AEROSPIKE_HOSTS`, `AEROSPIKE_NAMESPACE` |
| Minikube | `deployments/k8s/minikube/aerospike.yaml` |
| Compose | `deployments/docker/aerospike.conf`, `docker-compose.yml` |

## Verify a cache hit

1. Create or note a product id.
2. `GET /v1/products/{id}` twice with the same JWT.
3. First call may miss (Postgres); second should hit Aerospike — watch `helios_catalog_cache_hit{resource="product",result="hit"}` on catalog `/metrics`, or catalog logs/spans.
4. Optional: `kubectl -n helios exec deploy/aerospike -- asinfo -v 'sets/helios/products'` after traffic.

## Local vs production

| | Local (this repo) | Production shape |
|---|---|---|
| Topology | 1 node, memory storage `data-size` 256M | 3+ nodes, replication ≥ 2 |
| Role | Teach record cache + polyglot story | Same API; ops for rebalance, XDR optional |
| Failure | Pod restart empties memory namespace | Replicas keep records available |
