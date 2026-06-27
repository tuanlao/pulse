package snowflake

import "time"

// Strategy selects how a generator acquires its worker (node) id.
type Strategy string

const (
	// StrategyStatic takes the worker id from Config.WorkerID.Static. It is the
	// default — trivial and dependency-free, ideal for local/dev/test.
	StrategyStatic Strategy = "static"
	// StrategyStatefulSet derives the worker id from the StatefulSet pod ordinal
	// in the pod name (e.g. "web-3" -> 3). It errors for a Deployment, whose pod
	// name has a random suffix (e.g. "web-5d4b9c-xk2lp") with no usable number.
	StrategyStatefulSet Strategy = "statefulset"
	// StrategyRedis makes pods contend for a unique slot in [0, pool size) via a
	// redis lease, so any deployment topology gets distinct, recycled worker ids.
	StrategyRedis Strategy = "redis"
)

// defaultEpoch is Twitter's snowflake epoch (2010-11-04 01:42:54.657 UTC) in
// milliseconds. It matches bwmarrin/snowflake so ids are comparable across the
// Go ecosystem.
const defaultEpoch int64 = 1288834974657

// Config configures the snowflake id generator. Like every pulse package it is
// nested-object shaped (worker_id / redis / metrics sub-objects) and every field
// has a sensible default from DefaultConfig.
type Config struct {
	// Enabled toggles the generator. When false New returns a disabled generator
	// whose lifecycle methods are no-ops and whose Generate refuses to run. Default
	// true.
	Enabled bool `mapstructure:"enabled"`
	// Epoch is the custom epoch in Unix milliseconds; the 41-bit timestamp counts
	// from here (so it determines the ~69-year lifespan). 0 means the default
	// Twitter epoch — use WithEpochTime / a non-zero value to pick another.
	Epoch int64 `mapstructure:"epoch"`
	// NodeBits is the width of the worker-id field (Twitter default 10 => 1024
	// nodes). It bounds the redis pool size and the maximum StatefulSet ordinal.
	NodeBits uint8 `mapstructure:"node_bits"`
	// StepBits is the width of the per-millisecond sequence (Twitter default 12 =>
	// 4096 ids/ms/node). NodeBits+StepBits must be <= 22 (to keep a >=41-bit
	// timestamp); New errors otherwise.
	StepBits uint8 `mapstructure:"step_bits"`
	// MaxClockDriftWait bounds how long Generate waits for the wall clock to catch
	// back up after it moves backwards, before refusing (TryGenerate returns
	// ErrClockBackwards / Generate panics). Default 5ms.
	MaxClockDriftWait time.Duration `mapstructure:"max_clock_drift_wait"`

	WorkerID WorkerIDConfig `mapstructure:"worker_id"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

// WorkerIDConfig selects and configures the worker-id acquisition strategy.
type WorkerIDConfig struct {
	// Strategy is one of "static", "statefulset", "redis". Default "static".
	Strategy Strategy `mapstructure:"strategy"`
	// Static is the worker id used by the static strategy. Default 0.
	Static int64 `mapstructure:"static"`

	StatefulSet StatefulSetConfig `mapstructure:"statefulset"`
	Redis       RedisWorkerConfig `mapstructure:"redis"`
}

// StatefulSetConfig configures the statefulset strategy.
type StatefulSetConfig struct {
	// PodNameEnv is the environment variable holding the pod name (set it via the
	// downward API: metadata.name). When empty/unset the resolver falls back to
	// os.Hostname(). Default "POD_NAME".
	PodNameEnv string `mapstructure:"pod_name_env"`
}

// RedisWorkerConfig configures the redis strategy (slot lease + renewal).
type RedisWorkerConfig struct {
	// Redis is the connection used to build a dedicated worker-id client when no
	// shared rueidis client is supplied via Deps.RedisClient.
	Redis RedisConfig `mapstructure:"redis"`
	// KeyPrefix namespaces the per-slot lock keys. Default "pulse:snowflake:worker".
	KeyPrefix string `mapstructure:"key_prefix"`
	// TTL is the slot lease auto-expiry — the safety net that frees a crashed
	// pod's slot. Default 15s.
	TTL time.Duration `mapstructure:"ttl"`
	// RenewInterval is how often the lease is extended. 0 derives to TTL/3, which
	// tolerates two consecutive failed renewals before the lease can expire.
	RenewInterval time.Duration `mapstructure:"renew_interval"`
	// MaxID overrides the pool size (number of contended slots). 0 means the full
	// 2^NodeBits node space. A value larger than that is clamped.
	MaxID int `mapstructure:"max_id"`
}

// RedisConfig is a redis connection for the worker-id slot lease.
type RedisConfig struct {
	// Address is host:port. Default "localhost:6379".
	Address string `mapstructure:"address"`
	// Username for redis ACL auth.
	Username string `mapstructure:"username"`
	// Password for redis auth.
	Password string `mapstructure:"password"`
	// DB is the redis database number.
	DB int `mapstructure:"db"`
}

// MetricsConfig configures the package-owned Prometheus metrics.
type MetricsConfig struct {
	// Enabled toggles metrics. Default true (no-op unless a registry is wired via
	// NewMetrics + Deps.Metrics).
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "snowflake".
	Subsystem string `mapstructure:"subsystem"`
}

// DefaultConfig returns Config with sensible (Twitter-shaped) defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:           true,
		Epoch:             defaultEpoch,
		NodeBits:          10,
		StepBits:          12,
		MaxClockDriftWait: 5 * time.Millisecond,
		WorkerID: WorkerIDConfig{
			Strategy:    StrategyStatic,
			Static:      0,
			StatefulSet: StatefulSetConfig{PodNameEnv: "POD_NAME"},
			Redis: RedisWorkerConfig{
				Redis:     RedisConfig{Address: "localhost:6379"},
				KeyPrefix: "pulse:snowflake:worker",
				TTL:       15 * time.Second,
			},
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "pulse",
			Subsystem: "snowflake",
		},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Epoch == 0 {
		c.Epoch = d.Epoch
	}
	if c.NodeBits == 0 {
		c.NodeBits = d.NodeBits
	}
	if c.StepBits == 0 {
		c.StepBits = d.StepBits
	}
	if c.MaxClockDriftWait <= 0 {
		c.MaxClockDriftWait = d.MaxClockDriftWait
	}
	if c.WorkerID.Strategy == "" {
		c.WorkerID.Strategy = d.WorkerID.Strategy
	}
	if c.WorkerID.StatefulSet.PodNameEnv == "" {
		c.WorkerID.StatefulSet.PodNameEnv = d.WorkerID.StatefulSet.PodNameEnv
	}
	rw, dw := &c.WorkerID.Redis, d.WorkerID.Redis
	if rw.Redis.Address == "" {
		rw.Redis.Address = dw.Redis.Address
	}
	if rw.KeyPrefix == "" {
		rw.KeyPrefix = dw.KeyPrefix
	}
	if rw.TTL <= 0 {
		rw.TTL = dw.TTL
	}
	if rw.RenewInterval <= 0 {
		// Two missed renewals of slack before the lease can expire.
		rw.RenewInterval = rw.TTL / 3
	}
	if c.Metrics.Namespace == "" {
		c.Metrics.Namespace = d.Metrics.Namespace
	}
	if c.Metrics.Subsystem == "" {
		c.Metrics.Subsystem = d.Metrics.Subsystem
	}
}

// Option overrides Config fields programmatically (applied between two
// applyDefaults passes, so they win over defaults but unset fields still fill in).
type Option func(*Config)

// WithStaticWorkerID selects the static strategy with the given worker id.
func WithStaticWorkerID(id int64) Option {
	return func(c *Config) {
		c.WorkerID.Strategy = StrategyStatic
		c.WorkerID.Static = id
	}
}

// WithStatefulSetStrategy selects the statefulset (pod-ordinal) strategy.
func WithStatefulSetStrategy() Option {
	return func(c *Config) { c.WorkerID.Strategy = StrategyStatefulSet }
}

// WithRedisStrategy selects the redis slot-contention strategy. A non-empty
// address sets the dedicated worker-id client's address (ignored when a shared
// client is supplied via Deps.RedisClient).
func WithRedisStrategy(address string) Option {
	return func(c *Config) {
		c.WorkerID.Strategy = StrategyRedis
		if address != "" {
			c.WorkerID.Redis.Redis.Address = address
		}
	}
}

// WithNodeBits sets the worker-id field width.
func WithNodeBits(bits uint8) Option { return func(c *Config) { c.NodeBits = bits } }

// WithStepBits sets the per-millisecond sequence field width.
func WithStepBits(bits uint8) Option { return func(c *Config) { c.StepBits = bits } }

// WithEpoch sets the custom epoch in Unix milliseconds.
func WithEpoch(ms int64) Option { return func(c *Config) { c.Epoch = ms } }

// WithEpochTime sets the custom epoch from a time.Time.
func WithEpochTime(t time.Time) Option { return func(c *Config) { c.Epoch = t.UnixMilli() } }

// WithRedisTTL sets the redis slot lease TTL.
func WithRedisTTL(d time.Duration) Option {
	return func(c *Config) { c.WorkerID.Redis.TTL = d }
}

// WithPodNameEnv sets the env var the statefulset strategy reads the pod name from.
func WithPodNameEnv(name string) Option {
	return func(c *Config) { c.WorkerID.StatefulSet.PodNameEnv = name }
}

// WithKeyPrefix sets the redis slot-lock key prefix.
func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.WorkerID.Redis.KeyPrefix = prefix }
}
