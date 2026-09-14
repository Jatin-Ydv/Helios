package redisx

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Allow implements a sliding-window counter in Redis (ZSET of timestamps).
// Precision is millisecond. Cost: O(log N) per call. For gateway-edge limits
// this is the right default; token bucket is used for smoother shaping.
func Allow(ctx context.Context, c *redis.Client, key string, limit int, window time.Duration) (bool, error) {
	now := time.Now().UnixMilli()
	min := now - window.Milliseconds()
	pipe := c.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", min))
	zcard := pipe.ZCard(ctx, key)
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: fmt.Sprintf("%d-%d", now, now%1_000_000)})
	pipe.Expire(ctx, key, window+time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	return zcard.Val() < int64(limit), nil
}

// TokenBucket is a Redis HASH + Lua implementation of a leaky/token bucket.
// Returns remaining tokens after the take.
var bucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local take = tonumber(ARGV[4])
local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = cap
  ts = now
end
local delta = math.max(0, now - ts) / 1000.0
tokens = math.min(cap, tokens + delta * rate)
local allowed = 0
if tokens >= take then
  tokens = tokens - take
  allowed = 1
end
redis.call("HMSET", key, "tokens", tokens, "ts", now)
redis.call("PEXPIRE", key, 60000)
return {allowed, tokens}
`)

func TakeToken(ctx context.Context, c *redis.Client, key string, ratePerSec float64, burst int) (bool, float64, error) {
	res, err := bucketScript.Run(ctx, c, []string{key}, ratePerSec, burst, time.Now().UnixMilli(), 1).Slice()
	if err != nil {
		return false, 0, err
	}
	if len(res) < 2 {
		return false, 0, fmt.Errorf("token bucket: unexpected redis reply")
	}
	return asInt(res[0]) == 1, asFloat(res[1]), nil
}

func asInt(v any) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	case float64:
		return int64(t)
	default:
		return 0
	}
}

func asFloat(v any) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	default:
		return 0
	}
}
