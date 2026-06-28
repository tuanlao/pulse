package temporal

import (
	"github.com/tuanlao/pulse/pkg/temporal/client"
	"github.com/tuanlao/pulse/pkg/temporal/internal/tclient"
	"github.com/tuanlao/pulse/pkg/temporal/saga"
	"github.com/tuanlao/pulse/pkg/temporal/worker"
)

// Config is the composed Temporal configuration: one connection shared by the
// client and worker, plus client/worker behavior, the Continue-As-New history
// bounds, and the metrics/tracing toggles. A service embeds this in its own
// config struct (mapstructure-shaped) and loads it with pkg/config.
type Config struct {
	// Enabled gates the whole subsystem. When false NewClient/NewWorker return
	// disabled no-ops (safe to register). Default false so a demo runs without a
	// Temporal server.
	Enabled bool `mapstructure:"enabled"`
	// Connection is the Temporal frontend connection (host, namespace, identity,
	// TLS), shared by the client and the worker.
	Connection tclient.ConnectionConfig `mapstructure:"connection"`
	// Client configures the client component (start/signal/query helpers).
	Client client.Config `mapstructure:"client"`
	// Worker configures the worker component, including all the memory/OOM
	// controls (sticky cache, concurrency caps, resource tuner).
	Worker worker.Config `mapstructure:"worker"`
	// History bounds a single workflow run's event history (the Continue-As-New
	// guard). Services pass these thresholds into their workflow input and call
	// saga.ShouldContinueAsNew.
	History saga.Thresholds `mapstructure:"history"`
	// Metrics bridges the Temporal SDK's own metrics into pulse's shared Prometheus
	// registry via tally.
	Metrics MetricsConfig `mapstructure:"metrics"`
	// Tracing toggles the OTel tracing interceptor.
	Tracing TracingConfig `mapstructure:"tracing"`
}

// MetricsConfig configures the SDK→Prometheus metrics bridge.
type MetricsConfig struct {
	// Enabled toggles the tally→Prometheus metrics handler. Default true (active
	// only when a registry is wired via Deps.Registry).
	Enabled bool `mapstructure:"enabled"`
	// Prefix is the metric name prefix for SDK metrics. Default "temporal".
	Prefix string `mapstructure:"prefix"`
}

// TracingConfig toggles tracing.
type TracingConfig struct {
	// Enabled toggles the OTel tracing interceptor. Default true.
	Enabled bool `mapstructure:"enabled"`
}

// DefaultConfig returns Config with sensible defaults (disabled by default).
func DefaultConfig() Config {
	return Config{
		Enabled:    false,
		Connection: tclient.DefaultConnectionConfig(),
		Client:     client.DefaultConfig(),
		Worker:     worker.DefaultConfig(),
		History:    saga.DefaultThresholds(),
		Metrics:    MetricsConfig{Enabled: true, Prefix: "temporal"},
		Tracing:    TracingConfig{Enabled: true},
	}
}

func (c *Config) applyDefaults() {
	c.Connection.ApplyDefaults()
	c.Client.ApplyDefaults()
	c.Worker.ApplyDefaults()
	if c.Metrics.Prefix == "" {
		c.Metrics.Prefix = "temporal"
	}
	d := saga.DefaultThresholds()
	if c.History.MaxEvents <= 0 {
		c.History.MaxEvents = d.MaxEvents
	}
	if c.History.MaxBytes <= 0 {
		c.History.MaxBytes = d.MaxBytes
	}
}

// Option overrides the composed Config programmatically.
type Option func(*Config)

// WithEnabled toggles the whole subsystem.
func WithEnabled(b bool) Option { return func(c *Config) { c.Enabled = b } }

// WithHostPort sets the Temporal frontend address.
func WithHostPort(hostPort string) Option {
	return func(c *Config) { c.Connection.HostPort = hostPort }
}

// WithNamespace sets the Temporal namespace.
func WithNamespace(ns string) Option { return func(c *Config) { c.Connection.Namespace = ns } }

// WithTaskQueue sets the worker's task queue.
func WithTaskQueue(q string) Option { return func(c *Config) { c.Worker.TaskQueue = q } }
