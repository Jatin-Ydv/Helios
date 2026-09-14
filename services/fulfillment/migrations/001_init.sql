CREATE TABLE IF NOT EXISTS shipments (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    order_id     TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    status       TEXT NOT NULL, -- allocated | shipped | failed
    carrier      TEXT,
    tracking     TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, order_id)
);

CREATE TABLE IF NOT EXISTS inbox (
    event_id    TEXT PRIMARY KEY,
    consumer    TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
