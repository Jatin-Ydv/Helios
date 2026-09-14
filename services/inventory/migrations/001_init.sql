CREATE TABLE IF NOT EXISTS stock (
    tenant_id    TEXT NOT NULL,
    warehouse_id TEXT NOT NULL,
    sku          TEXT NOT NULL,
    on_hand      BIGINT NOT NULL DEFAULT 0,
    reserved     BIGINT NOT NULL DEFAULT 0,
    version      INT NOT NULL DEFAULT 1,
    PRIMARY KEY (tenant_id, warehouse_id, sku),
    CHECK (on_hand >= 0),
    CHECK (reserved >= 0),
    CHECK (reserved <= on_hand)
);

CREATE TABLE IF NOT EXISTS reservations (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    order_id      TEXT NOT NULL,
    warehouse_id  TEXT NOT NULL,
    sku           TEXT NOT NULL,
    quantity      BIGINT NOT NULL,
    status        TEXT NOT NULL, -- pending | committed | released | expired
    fence         BIGINT NOT NULL DEFAULT 0,
    expires_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, order_id, sku)
);

CREATE INDEX IF NOT EXISTS reservations_expiry_idx ON reservations (expires_at) WHERE status = 'pending';

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
