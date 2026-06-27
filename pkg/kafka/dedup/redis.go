package dedup

import (
	"context"
	"time"

	"github.com/redis/rueidis"
)

// redisDeduper remembers handled ids in redis, shared across pods. The existence
// check can optionally be served from rueidis client-side cache.
type redisDeduper struct {
	client   rueidis.Client
	prefix   string
	useCache bool
	cacheTTL time.Duration
}

func newRedis(cfg Config, client rueidis.Client) *redisDeduper {
	return &redisDeduper{
		client:   client,
		prefix:   cfg.Redis.KeyPrefix,
		useCache: cfg.Redis.ClientSideCache,
		cacheTTL: cfg.Redis.CacheTTL,
	}
}

func (d *redisDeduper) key(id string) string {
	if d.prefix == "" {
		return id
	}
	return d.prefix + ":" + id
}

func (d *redisDeduper) Seen(ctx context.Context, id string) (bool, error) {
	key := d.key(id)
	// GET (cacheable, unlike EXISTS) — a stored "1" means handled; redis-nil means
	// absent. The cached path is served from rueidis client-side cache.
	var resp rueidis.RedisResult
	if d.useCache {
		resp = d.client.DoCache(ctx, d.client.B().Get().Key(key).Cache(), d.cacheTTL)
	} else {
		resp = d.client.Do(ctx, d.client.B().Get().Key(key).Build())
	}
	if err := resp.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (d *redisDeduper) Mark(ctx context.Context, id string, ttl time.Duration) error {
	return d.client.Do(ctx, d.client.B().Set().Key(d.key(id)).Value("1").Px(ttl).Build()).Error()
}
