CREATE TABLE IF NOT EXISTS products (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    sku         TEXT NOT NULL,
    name        TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    amount      BIGINT NOT NULL,
    currency    TEXT NOT NULL DEFAULT 'USD',
    attributes  JSONB NOT NULL DEFAULT '{}'::jsonb,
    active      BOOLEAN NOT NULL DEFAULT TRUE,
    version     INT NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, sku)
);

CREATE INDEX IF NOT EXISTS products_tenant_idx ON products (tenant_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    TEXT NOT NULL,
    tenant_id       TEXT NOT NULL,
    payload         JSONB NOT NULL,
    headers         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ,
    publish_tries   INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (next_attempt_at) WHERE published_at IS NULL;

-- Debezium publication for log-based CDC of the outbox (production path).
-- The Go relay is the local/dev fallback that does not require Kafka Connect.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'helios_catalog_outbox') THEN
        CREATE PUBLICATION helios_catalog_outbox FOR TABLE outbox;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'skipping publication (need rds_replication / superuser in prod)';
END $$;
