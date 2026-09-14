package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Schema = `
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id    TEXT NOT NULL,
    key          TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status       TEXT NOT NULL,
    response     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, key)
);
`

var (
	ErrConflict = errors.New("idempotency: same key, different payload")
	ErrInFlight = errors.New("idempotency: request is already in flight")
)

type Record struct {
	Hash     string
	Status   string
	Response []byte
}

func Hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func Begin(ctx context.Context, tx pgx.Tx, tenantID, key, hash string) (*Record, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, key, request_hash, status)
		VALUES ($1,$2,$3,'started')
		ON CONFLICT DO NOTHING
	`, tenantID, key, hash)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil // first writer — proceed
	}
	var rec Record
	err = tx.QueryRow(ctx, `
		SELECT request_hash, status, COALESCE(response, '{}'::jsonb)
		FROM idempotency_keys WHERE tenant_id=$1 AND key=$2
	`, tenantID, key).Scan(&rec.Hash, &rec.Status, &rec.Response)
	if err != nil {
		return nil, err
	}
	if rec.Hash != hash {
		return nil, ErrConflict
	}
	if rec.Status == "started" {
		return nil, ErrInFlight
	}
	return &rec, nil
}

func Complete(ctx context.Context, tx pgx.Tx, tenantID, key string, response any) error {
	b, err := json.Marshal(response)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE idempotency_keys SET status='completed', response=$3
		WHERE tenant_id=$1 AND key=$2
	`, tenantID, key, b)
	return err
}

func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func PurgeExpired(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) error {
	_, err := pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE created_at < now() - $1::interval`, olderThan.String())
	return err
}
