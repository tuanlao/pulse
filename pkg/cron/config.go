// Package cron is a configurable job scheduler built on go-co-op/gocron/v2.
// Every job runs with structured logging and a tracing span (a trace id is
// generated when the context has none), panic recovery, optional per-job
// Prometheus metrics, an optional per-job timeout, and optional singleton /
// cross-pod (redis distributed lock) protection. The scheduler implements
// lifecycle.Component for ordered startup/shutdown.
//
// Like every pulse package it exposes Config + DefaultConfig() + functional
// Options, with nested-object config (lock/metrics sub-objects).
package cron

import "time"

// Config configures the scheduler.
type Config struct {
	// Enabled toggles the scheduler. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Timezone is the IANA location jobs are scheduled in. Default "UTC".
	Timezone string `mapstructure:"timezone"`
	// StopTimeout bounds graceful shutdown (waiting for running jobs). Default 30s.
	StopTimeout time.Duration `mapstructure:"stop_timeout"`
	// JobTimeout, when > 0, bounds every job run via a context deadline. A job's
	// own JobConfig.Timeout overrides this per job.
	JobTimeout time.Duration `mapstructure:"job_timeout"`
	// Singleton, when true, prevents a job from overlapping itself within this
	// process (gocron singleton mode, reschedule). Default true.
	Singleton bool `mapstructure:"singleton"`

	// Jobs declares jobs in config (schedule here, handler registered in code via
	// Scheduler.Register). Keyed by job name.
	Jobs map[string]JobConfig `mapstructure:"jobs"`

	Lock    LockConfig    `mapstructure:"lock"`
	Metrics MetricsConfig `mapstructure:"metrics"`
}

// JobConfig declares a job's schedule in configuration. The handler is bound in
// code with Scheduler.Register(name, fn).
type JobConfig struct {
	// Enabled must be true for the job to be scheduled.
	Enabled bool `mapstructure:"enabled"`
	// Cron is a crontab spec (mutually exclusive with Every).
	Cron string `mapstructure:"cron"`
	// WithSeconds enables the 6-field crontab format (seconds).
	WithSeconds bool `mapstructure:"with_seconds"`
	// Every schedules at a fixed interval (used when Cron is empty).
	Every time.Duration `mapstructure:"every"`
	// Timeout overrides the global JobTimeout for this job (0 = use global).
	Timeout time.Duration `mapstructure:"timeout"`
}

// LockConfig configures a redis distributed lock so that each scheduled run is
// executed by exactly one pod. With multiple pods racing for the per-job lock,
// runs are spread across pods (load distribution) rather than concentrated on a
// single leader. Opt-in (disabled by default) so local/dev runs need no redis.
type LockConfig struct {
	// Enabled toggles the distributed lock. Default false.
	Enabled bool `mapstructure:"enabled"`
	// Redis is the connection used to build the locker.
	Redis RedisConfig `mapstructure:"redis"`
	// KeyPrefix namespaces lock keys (avoid collisions across services). Default
	// "pulse:cron".
	KeyPrefix string `mapstructure:"key_prefix"`
	// Tries is how many times a pod attempts to acquire the lock before skipping
	// the run (1 = skip immediately if another pod holds it). Default 1.
	Tries int `mapstructure:"tries"`
}

// RedisConfig is a redis connection for the cron distributed lock.
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

// MetricsConfig configures per-job Prometheus metrics.
type MetricsConfig struct {
	// Enabled toggles metrics. Default true (no-op unless a registry is wired).
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "cron".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the run-duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:     true,
		Timezone:    "UTC",
		StopTimeout: 30 * time.Second,
		JobTimeout:  0,
		Singleton:   true,
		Lock: LockConfig{
			Enabled:   false,
			Redis:     RedisConfig{Address: "localhost:6379"},
			KeyPrefix: "pulse:cron",
			Tries:     1,
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "pulse",
			Subsystem: "cron",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300},
		},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Timezone == "" {
		c.Timezone = d.Timezone
	}
	if c.StopTimeout <= 0 {
		c.StopTimeout = d.StopTimeout
	}
	if c.Lock.Redis.Address == "" {
		c.Lock.Redis.Address = d.Lock.Redis.Address
	}
	if c.Lock.KeyPrefix == "" {
		c.Lock.KeyPrefix = d.Lock.KeyPrefix
	}
	if c.Lock.Tries <= 0 {
		c.Lock.Tries = d.Lock.Tries
	}
	if c.Metrics.Namespace == "" {
		c.Metrics.Namespace = d.Metrics.Namespace
	}
	if c.Metrics.Subsystem == "" {
		c.Metrics.Subsystem = d.Metrics.Subsystem
	}
	if len(c.Metrics.Buckets) == 0 {
		c.Metrics.Buckets = d.Metrics.Buckets
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithTimezone sets the scheduler timezone (IANA location name).
func WithTimezone(tz string) Option { return func(c *Config) { c.Timezone = tz } }

// WithJobTimeout sets a per-job context timeout.
func WithJobTimeout(d time.Duration) Option { return func(c *Config) { c.JobTimeout = d } }

// WithSingleton toggles single-flight (non-overlapping) job execution.
func WithSingleton(enabled bool) Option { return func(c *Config) { c.Singleton = enabled } }

// WithLock enables the redis distributed lock (per-job, spreading runs across
// pods) using the given redis address.
func WithLock(address string) Option {
	return func(c *Config) {
		c.Lock.Enabled = true
		c.Lock.Redis.Address = address
	}
}
