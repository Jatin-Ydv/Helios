package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/helios-commerce/helios/platform/events"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Schema = `
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
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (next_attempt_at)
    WHERE published_at IS NULL;
`

// Insert writes an event into the outbox in the SAME transaction as the
// business write. This is the transactional outbox pattern: either both commit
// or neither does. Kafka is not a participant in the SQL transaction.
func Insert(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	headers, err := json.Marshal(env.Trace)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (id, event_type, aggregate_type, aggregate_id, tenant_id, payload, headers)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
	`, env.EventID, env.EventType, env.AggregateType, env.AggregateID, env.TenantID, body, headers)
	return err
}

type Row struct {
	ID      string
	Payload []byte
	Type    string
	Tries   int
}

func ClaimBatch(ctx context.Context, pool *pgxpool.Pool, limit int) ([]Row, error) {
	rows, err := pool.Query(ctx, `
		WITH cte AS (
			SELECT id FROM outbox
			WHERE published_at IS NULL AND next_attempt_at <= now()
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox o
		SET publish_tries = o.publish_tries + 1,
		    next_attempt_at = now() + (INTERVAL '1 second' * LEAST(power(2, o.publish_tries), 60))
		FROM cte WHERE o.id = cte.id
		RETURNING o.id, o.payload, o.event_type, o.publish_tries
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		if err := rows.Scan(&r.ID, &r.Payload, &r.Type, &r.Tries); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func MarkPublished(ctx context.Context, pool *pgxpool.Pool, id string) error {
	_, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, id)
	return err
}

func UnpublishedCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

func Age(ctx context.Context, pool *pgxpool.Pool) (time.Duration, error) {
	var t *time.Time
	err := pool.QueryRow(ctx, `SELECT min(created_at) FROM outbox WHERE published_at IS NULL`).Scan(&t)
	if err != nil || t == nil {
		return 0, err
	}
	return time.Since(*t), nil
}
