package redisx

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type Cache struct {
	c *redis.Client
}

func NewCache(c *redis.Client) *Cache { return &Cache{c: c} }

func (k *Cache) Get(ctx context.Context, key string, dest any) (bool, error) {
	s, err := k.c.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal([]byte(s), dest)
}

func (k *Cache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return k.c.Set(ctx, key, b, ttl).Err()
}

func (k *Cache) Del(ctx context.Context, keys ...string) error {
	return k.c.Del(ctx, keys...).Err()
}

// Singleflight-ish: SETNX a placeholder so thundering herds wait rather than
// all hitting Postgres. Callers that lose the race should GET again shortly.
func (k *Cache) BeginFill(ctx context.Context, key string, ttl time.Duration) (won bool, err error) {
	return k.c.SetNX(ctx, key+":lock", "1", ttl).Result()
}
