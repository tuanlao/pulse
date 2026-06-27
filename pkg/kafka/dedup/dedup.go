// Package dedup provides best-effort message deduplication for kafka consumers.
// A message id (the x-message-id header) is checked before processing and marked
// only after a successful handle, so a redelivered duplicate is skipped while a
// failed message is still retried. Two backends: local (an in-process otter
// cache, per pod) and redis (shared across pods, optionally served from rueidis
// client-side cache). It is opt-in (disabled by default).
//
// Deduplication is best-effort de-amplification, NOT exactly-once: two truly
// concurrent deliveries can both pass the check. The TTL must exceed the maximum
// retry lifetime so an in-flight message's id does not expire mid-retry.
package dedup

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// Config configures deduplication.
type Config struct {
	// Enabled toggles dedup. Default false (opt-in).
	Enabled bool `mapstructure:"enabled"`
	// Mode selects the backend: "local" (otter, per-pod) or "redis" (shared).
	// Default "local".
	Mode string `mapstructure:"mode"`
	// TTL is how long a processed id is remembered. It MUST exceed the maximum
	// retry lifetime. Default 1h.
	TTL time.Duration `mapstructure:"ttl"`

	Local LocalConfig `mapstructure:"local"`
	Redis RedisConfig `mapstructure:"redis"`
}

// LocalConfig configures the in-process otter cache.
type LocalConfig struct {
	// MaxSize caps the number of remembered ids. Default 100000.
	MaxSize int `mapstructure:"max_size"`
}

// RedisConfig configures the shared redis backend.
type RedisConfig struct {
	// KeyPrefix namespaces dedup keys. Default "pulse:kafka:dedup".
	KeyPrefix string `mapstructure:"key_prefix"`
	// ClientSideCache serves the existence check from rueidis client-side cache
	// (DoCache) — useful because the same id is checked again at each retry tier.
	// Requires the shared redis client to have caching enabled (BCAST tracking on
	// the prefix keeps it fresh). Default false.
	ClientSideCache bool `mapstructure:"client_side_cache"`
	// CacheTTL bounds the client-side cache entry lifetime. Default 30s.
	CacheTTL time.Duration `mapstructure:"cache_ttl"`
}

// DefaultConfig returns dedup defaults.
func DefaultConfig() Config {
	return Config{
		Enabled: false,
		Mode:    "local",
		TTL:     time.Hour,
		Local:   LocalConfig{MaxSize: 100_000},
		Redis:   RedisConfig{KeyPrefix: "pulse:kafka:dedup", ClientSideCache: false, CacheTTL: 30 * time.Second},
	}
}

// ApplyDefaults fills empty fields from DefaultConfig.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.Mode == "" {
		c.Mode = d.Mode
	}
	if c.TTL <= 0 {
		c.TTL = d.TTL
	}
	if c.Local.MaxSize <= 0 {
		c.Local.MaxSize = d.Local.MaxSize
	}
	if c.Redis.KeyPrefix == "" {
		c.Redis.KeyPrefix = d.Redis.KeyPrefix
	}
	if c.Redis.CacheTTL <= 0 {
		c.Redis.CacheTTL = d.Redis.CacheTTL
	}
}

// Deduper records and reports whether a message id has been handled.
type Deduper interface {
	// Seen reports whether id was already marked as handled.
	Seen(ctx context.Context, id string) (bool, error)
	// Mark records id as handled for ttl.
	Mark(ctx context.Context, id string, ttl time.Duration) error
}

// New builds a Deduper from cfg. It returns (nil, nil) when disabled, so callers
// can treat a nil Deduper as "no dedup". The redis mode requires a non-nil
// rueidis client (fail-fast otherwise).
func New(cfg Config, redisClient rueidis.Client) (Deduper, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg.ApplyDefaults()
	switch cfg.Mode {
	case "local":
		return newLocal(cfg)
	case "redis":
		if redisClient == nil {
			return nil, fmt.Errorf("kafka: dedup mode %q requires a redis client (set Deps.RedisClient)", cfg.Mode)
		}
		return newRedis(cfg, redisClient), nil
	default:
		return nil, fmt.Errorf("kafka: unknown dedup mode %q (want local|redis)", cfg.Mode)
	}
}
