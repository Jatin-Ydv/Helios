package inbox

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const Schema = `
CREATE TABLE IF NOT EXISTS inbox (
    event_id     TEXT PRIMARY KEY,
    consumer     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

var ErrDuplicate = errors.New("inbox: duplicate event")

// TryClaim inserts the event id. A unique-violation means this consumer already
// processed (or is processing) the message — at-least-once delivery becomes
// effectively-once at the application layer.
func TryClaim(ctx context.Context, tx pgx.Tx, consumer, eventID, eventType string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO inbox (event_id, consumer, event_type) VALUES ($1,$2,$3)
	`, eventID, consumer, eventType)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func IsDuplicate(err error) bool { return errors.Is(err, ErrDuplicate) }
