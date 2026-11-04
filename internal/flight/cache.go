package flight

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Cache struct {
	client redis.UniversalClient
	ttl    time.Duration
	log    *slog.Logger
}

func NewCache(client redis.UniversalClient, ttl time.Duration, log *slog.Logger) *Cache {
	return &Cache{client: client, ttl: ttl, log: log}
}

func (c *Cache) get(ctx context.Context, key string, dst proto.Message) bool {
	if c == nil {
		return false
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	b, err := c.client.Get(cacheCtx, key).Bytes()
	if err == nil && protojson.Unmarshal(b, dst) == nil {
		c.log.Info("cache hit", "key", key)
		return true
	}
	c.log.Info("cache miss", "key", key)
	if err != nil && err != redis.Nil {
		c.log.Warn("redis unavailable; using PostgreSQL", "error", err)
	}
	return false
}

func (c *Cache) set(ctx context.Context, key string, value proto.Message) {
	if c == nil {
		return
	}
	b, err := protojson.Marshal(value)
	if err != nil {
		return
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if err = c.client.Set(cacheCtx, key, b, c.ttl).Err(); err != nil {
		c.log.Warn("cache write failed", "error", err)
	}
}
