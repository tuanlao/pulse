package kafka

import (
	"time"

	"github.com/tuanlao/pulse/pkg/env"
	"github.com/tuanlao/pulse/pkg/kafka/admin"
	"github.com/tuanlao/pulse/pkg/kafka/consumer"
	"github.com/tuanlao/pulse/pkg/kafka/dedup"
	"github.com/tuanlao/pulse/pkg/kafka/internal/kclient"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	"github.com/tuanlao/pulse/pkg/kafka/producer"
	"github.com/tuanlao/pulse/pkg/kafka/retry"
	ktrace "github.com/tuanlao/pulse/pkg/kafka/trace"
)

// Config is the composed kafka configuration. Each branch is a named sub-config
// owned (and documented) by its own package, so this facade stays a thin
// composition. A service embeds Config in its own config struct and loads it with
// pkg/config (defaults < config.yaml).
type Config struct {
	// Enabled toggles the component. When false, NewProducer/NewConsumer return
	// no-op components so registering them is always safe. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Cross-cutting values propagated into sub-configs by the service.
	Env         env.Env `mapstructure:"env"`
	ServiceName string  `mapstructure:"service_name"`
	Version     string  `mapstructure:"version"`

	Client   kclient.Config  `mapstructure:"client"`
	Producer producer.Config `mapstructure:"producer"`
	Consumer consumer.Config `mapstructure:"consumer"`
	Admin    admin.Config    `mapstructure:"admin"`
	Retry    retry.Config    `mapstructure:"retry"`
	Dedup    dedup.Config    `mapstructure:"dedup"`
	Metrics  kmetrics.Config `mapstructure:"metrics"`
	Trace    ktrace.Config   `mapstructure:"trace"`
}

// DefaultConfig composes each sub-config's defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:  true,
		Env:      env.EnvDev,
		Client:   kclient.DefaultConfig(),
		Producer: producer.DefaultConfig(),
		Consumer: consumer.DefaultConfig(),
		Admin:    admin.DefaultConfig(),
		Retry:    retry.DefaultConfig(),
		Dedup:    dedup.DefaultConfig(),
		Metrics:  kmetrics.DefaultConfig(),
		Trace:    ktrace.DefaultConfig(),
	}
}

func (c *Config) applyDefaults() {
	c.Env = c.Env.Normalize()
	c.Client.ApplyDefaults()
	c.Producer.ApplyDefaults()
	c.Consumer.ApplyDefaults()
	c.Admin.ApplyDefaults()
	c.Retry.ApplyDefaults()
	c.Dedup.ApplyDefaults()
	c.Metrics.ApplyDefaults(kmetrics.DefaultConfig())
}

// source identifies the producing service (stamped as the x-source header).
func (c Config) source() string {
	switch {
	case c.ServiceName != "" && c.Version != "":
		return c.ServiceName + "@" + c.Version
	default:
		return c.ServiceName
	}
}

// Option overrides Config fields programmatically (applied after defaults).
type Option func(*Config)

// WithBrokers sets the seed brokers.
func WithBrokers(brokers ...string) Option {
	return func(c *Config) { c.Client.Brokers = brokers }
}

// WithClientID sets the client id.
func WithClientID(id string) Option { return func(c *Config) { c.Client.ClientID = id } }

// WithGroupID sets the consumer group id.
func WithGroupID(id string) Option { return func(c *Config) { c.Consumer.GroupID = id } }

// WithTopics sets the consumer's origin topics.
func WithTopics(topics ...string) Option {
	return func(c *Config) { c.Consumer.Topics = topics }
}

// WithMode sets the consumer processing mode (unordered/ordered/key_ordered).
func WithMode(mode string) Option { return func(c *Config) { c.Consumer.Mode = mode } }

// WithConcurrency sets the consumer worker concurrency.
func WithConcurrency(n int) Option { return func(c *Config) { c.Consumer.Concurrency = n } }

// WithCommitImmediately toggles at-most-once immediate commits.
func WithCommitImmediately(v bool) Option {
	return func(c *Config) { c.Consumer.CommitImmediately = v }
}

// WithBackoffs sets the retry tier delays.
func WithBackoffs(backoffs ...time.Duration) Option {
	return func(c *Config) { c.Retry.Backoffs = backoffs }
}

// WithStrategy sets the retry strategy (auto/blocking/non_blocking).
func WithStrategy(s string) Option { return func(c *Config) { c.Retry.Strategy = s } }

// WithAutoCreateTopics toggles topic auto-creation.
func WithAutoCreateTopics(v bool) Option { return func(c *Config) { c.Admin.AutoCreate = v } }

// WithPartitions sets the partition count for created topics.
func WithPartitions(n int32) Option { return func(c *Config) { c.Admin.Partitions = n } }

// WithReplicationFactor sets the replication factor for created topics.
func WithReplicationFactor(n int16) Option {
	return func(c *Config) { c.Admin.ReplicationFactor = n }
}

// WithDedup enables deduplication in the given mode (local/redis).
func WithDedup(mode string) Option {
	return func(c *Config) {
		c.Dedup.Enabled = true
		c.Dedup.Mode = mode
	}
}

// WithDLQ toggles the dead-letter queue.
func WithDLQ(v bool) Option { return func(c *Config) { c.Retry.DLQ.Enabled = v } }
