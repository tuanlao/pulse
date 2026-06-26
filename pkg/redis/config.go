// Package redis is a configurable redis component built on rueidis. Its headline
// feature is rueidis client-side caching, including the cheap, scalable
// broadcast (BCAST) prefix tracking mode — both fully declarable in config.
//
// A single addresses list covers standalone (one address) and cluster (many,
// auto-discovered) topologies; Sentinel and TLS are opt-in sub-objects. The
// Client embeds rueidis.Client (so callers use B()/Do/DoCache directly) and
// implements lifecycle.Component for ordered startup/shutdown. Commands are
// optionally wrapped with OTel spans and package-owned Prometheus metrics.
//
// Like every pulse package it exposes Config + DefaultConfig() + functional
// Options, with nested-object config (tls/cache/sentinel/metrics sub-objects).
package redis

import "time"

// Config configures the redis client.
type Config struct {
	// Enabled toggles the component. When false, New returns a disabled Client
	// whose lifecycle methods are no-ops and which never dials. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Addresses are the redis nodes (host:port). One address = standalone; more
	// than one = cluster (auto-discovered). When Sentinel.Enabled, these address
	// the sentinels. Default ["localhost:6379"].
	Addresses []string `mapstructure:"addresses"`
	// Username for redis ACL auth.
	Username string `mapstructure:"username"`
	// Password for redis auth.
	Password string `mapstructure:"password"`
	// DB is the redis database number (SELECT). Ignored in cluster mode.
	DB int `mapstructure:"db"`
	// ClientName sets CLIENT SETNAME for observability on the server side.
	ClientName string `mapstructure:"client_name"`

	// DialTimeout bounds establishing a TCP connection. Default 5s.
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	// ConnWriteTimeout is the per-connection read/write timeout (also drives the
	// periodic PING liveness check). Default 10s.
	ConnWriteTimeout time.Duration `mapstructure:"conn_write_timeout"`

	// SendToReplicas, when true, routes read-only commands to replicas (cluster
	// mode). Default false.
	SendToReplicas bool `mapstructure:"send_to_replicas"`
	// PingOnStart, when true, makes Start issue a PING as a readiness gate so a
	// broken connection fails startup fast. Default true.
	PingOnStart bool `mapstructure:"ping_on_start"`

	// Tuning knobs. Zero means "use the rueidis default" (PipelineMultiplex uses
	// -1 to disable multiplexing).
	BlockingPoolSize  int           `mapstructure:"blocking_pool_size"`
	PipelineMultiplex int           `mapstructure:"pipeline_multiplex"`
	RingScaleEachConn int           `mapstructure:"ring_scale_each_conn"`
	MaxFlushDelay     time.Duration `mapstructure:"max_flush_delay"`

	TLS      TLSConfig      `mapstructure:"tls"`
	Cache    CacheConfig    `mapstructure:"cache"`
	Sentinel SentinelConfig `mapstructure:"sentinel"`
	Tracing  TracingConfig  `mapstructure:"tracing"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

// TLSConfig configures TLS for the redis connection. Opt-in.
type TLSConfig struct {
	// Enabled toggles TLS. Default false.
	Enabled bool `mapstructure:"enabled"`
	// InsecureSkipVerify disables certificate verification (dev only). Default false.
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
	// CAFile is a PEM bundle to verify the server certificate against.
	CAFile string `mapstructure:"ca_file"`
	// CertFile / KeyFile are the client certificate for mutual TLS.
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	// ServerName overrides the SNI / verification hostname.
	ServerName string `mapstructure:"server_name"`
}

// CacheConfig configures rueidis client-side caching — the headline feature.
// Caching requires RESP3 (redis >= 6). It is on by default; reads served from
// the local cache via DoCache avoid a round trip until invalidated.
type CacheConfig struct {
	// Enabled toggles client-side caching. When false, DoCache transparently
	// falls back to a normal round trip (ClientOption.DisableCache). Default true.
	Enabled bool `mapstructure:"enabled"`
	// SizePerConn is the client-side cache size (bytes) bound to each connection.
	// 0 = rueidis default (128 MiB).
	SizePerConn int `mapstructure:"size_per_conn"`
	// Broadcast configures BCAST-mode invalidation tracking by key prefix.
	Broadcast BroadcastConfig `mapstructure:"broadcast"`
}

// BroadcastConfig enables CLIENT TRACKING broadcast (BCAST) mode: the server
// proactively pushes invalidations for the configured key prefixes, instead of
// per-key opt-in tracking. This is the cheapest way to keep a hot, well-prefixed
// keyspace cached client-side.
type BroadcastConfig struct {
	// Enabled toggles BCAST mode. Requires at least one prefix. Default false
	// (prefixes are application-specific), but fully enableable here.
	Enabled bool `mapstructure:"enabled"`
	// Prefixes are the key prefixes the server broadcasts invalidations for
	// (e.g. ["user:", "product:"]). Required when Enabled.
	Prefixes []string `mapstructure:"prefixes"`
}

// SentinelConfig configures redis Sentinel. Opt-in; Addresses then point at the
// sentinels rather than at redis nodes directly.
type SentinelConfig struct {
	// Enabled toggles Sentinel mode. Default false.
	Enabled bool `mapstructure:"enabled"`
	// MasterSet is the monitored master set name. Default "mymaster" when enabled.
	MasterSet string `mapstructure:"master_set"`
	// Username / Password authenticate to the sentinels.
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// TracingConfig configures per-command OTel spans.
type TracingConfig struct {
	// Enabled toggles command spans (requires a non-nil Deps.TracerProvider to
	// actually export). Default true.
	Enabled bool `mapstructure:"enabled"`
}

// MetricsConfig configures per-command Prometheus metrics.
type MetricsConfig struct {
	// Enabled toggles metrics (no-op unless Deps.Metrics is wired). Default true.
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "redis".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the command-duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:          true,
		Addresses:        []string{"localhost:6379"},
		DialTimeout:      5 * time.Second,
		ConnWriteTimeout: 10 * time.Second,
		PingOnStart:      true,
		TLS:              TLSConfig{Enabled: false},
		Cache: CacheConfig{
			Enabled:   true,
			Broadcast: BroadcastConfig{Enabled: false},
		},
		Sentinel: SentinelConfig{Enabled: false},
		Tracing:  TracingConfig{Enabled: true},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "pulse",
			Subsystem: "redis",
			Buckets:   []float64{0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if len(c.Addresses) == 0 {
		c.Addresses = d.Addresses
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = d.DialTimeout
	}
	if c.ConnWriteTimeout <= 0 {
		c.ConnWriteTimeout = d.ConnWriteTimeout
	}
	if c.Sentinel.Enabled && c.Sentinel.MasterSet == "" {
		c.Sentinel.MasterSet = "mymaster"
	}
	c.Metrics.applyDefaults(d.Metrics)
}

// applyDefaults fills empty Prometheus namespace/subsystem/buckets from d. Shared
// by Config.applyDefaults and NewMetrics so the metric defaults live in one place.
func (m *MetricsConfig) applyDefaults(d MetricsConfig) {
	if m.Namespace == "" {
		m.Namespace = d.Namespace
	}
	if m.Subsystem == "" {
		m.Subsystem = d.Subsystem
	}
	if len(m.Buckets) == 0 {
		m.Buckets = d.Buckets
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithAddresses sets the redis node (or sentinel) addresses.
func WithAddresses(addrs ...string) Option {
	return func(c *Config) { c.Addresses = addrs }
}

// WithCredentials sets the username and password.
func WithCredentials(username, password string) Option {
	return func(c *Config) {
		c.Username = username
		c.Password = password
	}
}

// WithDB selects the redis database number.
func WithDB(db int) Option { return func(c *Config) { c.DB = db } }

// WithClientName sets CLIENT SETNAME.
func WithClientName(name string) Option { return func(c *Config) { c.ClientName = name } }

// WithClientCache toggles client-side caching.
func WithClientCache(enabled bool) Option {
	return func(c *Config) { c.Cache.Enabled = enabled }
}

// WithBroadcast enables client-side caching in broadcast (BCAST) mode for the
// given key prefixes — the cheapest way to cache a well-prefixed hot keyspace.
func WithBroadcast(prefixes ...string) Option {
	return func(c *Config) {
		c.Cache.Enabled = true
		c.Cache.Broadcast.Enabled = true
		c.Cache.Broadcast.Prefixes = prefixes
	}
}

// WithSendToReplicas routes read-only commands to replicas (cluster mode).
func WithSendToReplicas(enabled bool) Option {
	return func(c *Config) { c.SendToReplicas = enabled }
}

// WithSentinel enables Sentinel mode for the given master set (Addresses then
// point at the sentinels).
func WithSentinel(masterSet string) Option {
	return func(c *Config) {
		c.Sentinel.Enabled = true
		c.Sentinel.MasterSet = masterSet
	}
}

// WithTLS sets the TLS sub-config.
func WithTLS(tls TLSConfig) Option { return func(c *Config) { c.TLS = tls } }
