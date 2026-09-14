CREATE TABLE IF NOT EXISTS notifications (
    id           TEXT PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    channel      TEXT NOT NULL,
    recipient    TEXT NOT NULL,
    template     TEXT NOT NULL,
    payload      JSONB NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS notifications_tenant_idx ON notifications (tenant_id, delivered_at DESC);
