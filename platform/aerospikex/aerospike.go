// Package aerospikex wraps the Aerospike Go client for Helios caches.
// Catalog product records live here; Redis remains for locks and rate limits.
package aerospikex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v7"
)

const (
	binJSON = "json"
	setProducts = "products"
)

// Connect builds a single shared client from comma-separated host:port list.
func Connect(hostsCSV string) (*as.Client, error) {
	hostsCSV = strings.TrimSpace(hostsCSV)
	if hostsCSV == "" {
		return nil, errors.New("aerospike: empty AEROSPIKE_HOSTS")
	}
	var hosts []*as.Host
	for _, part := range strings.Split(hostsCSV, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		h, p, err := net.SplitHostPort(part)
		if err != nil {
			return nil, fmt.Errorf("aerospike host %q: %w", part, err)
		}
		port, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("aerospike port %q: %w", p, err)
		}
		hosts = append(hosts, as.NewHost(h, port))
	}
	if len(hosts) == 0 {
		return nil, errors.New("aerospike: no hosts parsed")
	}
	policy := as.NewClientPolicy()
	policy.Timeout = 5 * time.Second
	policy.ConnectionQueueSize = 32
	client, err := as.NewClientWithPolicyAndHost(policy, hosts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func Ping(ctx context.Context, c *as.Client) error {
	if c == nil || !c.IsConnected() {
		return errors.New("aerospike: not connected")
	}
	done := make(chan error, 1)
	go func() {
		nodes := c.GetNodes()
		if len(nodes) == 0 {
			done <- errors.New("aerospike: no nodes")
			return
		}
		done <- nil
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// Cache stores JSON-serialized values under a primary key in namespace/set.
type Cache struct {
	client    *as.Client
	namespace string
	set       string
}

func NewCache(client *as.Client, namespace string) *Cache {
	if namespace == "" {
		namespace = "helios"
	}
	return &Cache{client: client, namespace: namespace, set: setProducts}
}

func (k *Cache) key(userKey string) (*as.Key, error) {
	return as.NewKey(k.namespace, k.set, userKey)
}

func (k *Cache) Get(ctx context.Context, userKey string, dest any) (bool, error) {
	_ = ctx
	ak, err := k.key(userKey)
	if err != nil {
		return false, err
	}
	rec, err := k.client.Get(nil, ak, binJSON)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if rec == nil || rec.Bins == nil {
		return false, nil
	}
	raw, ok := rec.Bins[binJSON].(string)
	if !ok {
		if b, ok := rec.Bins[binJSON].([]byte); ok {
			raw = string(b)
		} else {
			return false, fmt.Errorf("aerospike: unexpected bin type %T", rec.Bins[binJSON])
		}
	}
	return true, json.Unmarshal([]byte(raw), dest)
}

func (k *Cache) Set(ctx context.Context, userKey string, v any, ttl time.Duration) error {
	_ = ctx
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ak, err := k.key(userKey)
	if err != nil {
		return err
	}
	sec := uint32(ttl.Seconds())
	if sec == 0 && ttl > 0 {
		sec = 1
	}
	wp := as.NewWritePolicy(0, sec)
	return k.client.PutBins(wp, ak, as.NewBin(binJSON, string(body)))
}

func (k *Cache) Del(ctx context.Context, keys ...string) error {
	_ = ctx
	var first error
	for _, userKey := range keys {
		ak, err := k.key(userKey)
		if err != nil {
			if first == nil {
				first = err
			}
			continue
		}
		_, err = k.client.Delete(nil, ak)
		if err != nil && !isNotFound(err) && first == nil {
			first = err
		}
	}
	return first
}

func isNotFound(err error) bool {
	return errors.Is(err, as.ErrKeyNotFound)
}
