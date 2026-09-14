CREATE TABLE IF NOT EXISTS payments (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    order_id      TEXT NOT NULL,
    amount        BIGINT NOT NULL,
    currency      TEXT NOT NULL,
    status        TEXT NOT NULL, -- authorized | captured | voided | failed
    psp_reference TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
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

CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id    TEXT NOT NULL,
    key          TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status       TEXT NOT NULL,
    response     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);
