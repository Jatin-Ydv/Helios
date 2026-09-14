package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrLockNotHeld = errors.New("redis lock: not held")

func Connect(addr, password string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           0,
		ReadTimeout:  400 * time.Millisecond,
		WriteTimeout: 400 * time.Millisecond,
		PoolSize:     32,
		MinIdleConns: 8,
	})
}

func Ping(ctx context.Context, c *redis.Client) error {
	return c.Ping(ctx).Err()
}

// Lock is a fencing-aware lock. The token MUST be stored next to the critical
// section's write (e.g. reservation.fence) so a zombie holder cannot overwrite
// after expiry. See "How to do distributed locking" — Martin Kleppmann.
type Lock struct {
	client *redis.Client
	key    string
	token  string
	ttl    time.Duration
}

var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

var extendScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)

func Acquire(ctx context.Context, c *redis.Client, key string, ttl time.Duration) (*Lock, error) {
	tok, err := randomToken()
	if err != nil {
		return nil, err
	}
	ok, err := c.SetNX(ctx, key, tok, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("redis lock: busy %s", key)
	}
	return &Lock{client: c, key: key, token: tok, ttl: ttl}, nil
}

func (l *Lock) Token() string { return l.token }

func (l *Lock) Extend(ctx context.Context) error {
	n, err := extendScript.Run(ctx, l.client, []string{l.key}, l.token, l.ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLockNotHeld
	}
	return nil
}

func (l *Lock) Unlock(ctx context.Context) error {
	n, err := unlockScript.Run(ctx, l.client, []string{l.key}, l.token).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLockNotHeld
	}
	return nil
}

func randomToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Fence increments a per-resource monotonic token. Compare-and-set the fence
// on the durable row; ignore writes whose fence is stale.
func Fence(ctx context.Context, c *redis.Client, resource string) (int64, error) {
	return c.Incr(ctx, "fence:"+resource).Result()
}
