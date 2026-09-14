// Package id issues globally unique, lexicographically sortable identifiers.
//
// ULIDs are preferred over UUIDv4 for aggregates that are stored in B-tree
// indexes: they are time-ordered, which avoids random page splits under write
// load (a real production issue at order-creation QPS).
package id

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

var (
	mu      sync.Mutex
	entropy io.Reader = ulid.Monotonic(rand.Reader, 0)
)

// NewULID returns a new ULID string (26 Crockford base32 characters).
func NewULID() string {
	mu.Lock()
	defer mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now().UTC()), entropy).String()
}

// NewUUIDv7 returns an RFC 9562 UUIDv7 (time-ordered). Used where downstream
// systems expect UUID formatting (payments, external PSP correlation).
func NewUUIDv7() string {
	return uuid.Must(uuid.NewV7()).String()
}

// NewUUID returns a random UUIDv4. Prefer NewULID or NewUUIDv7 for new aggregates.
func NewUUID() string {
	return uuid.NewString()
}
