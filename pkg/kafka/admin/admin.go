// Package admin provisions Kafka topics for pkg/kafka. With AutoCreate it
// idempotently creates the origin topics (plus the consumer's retry tiers and
// DLQ) at the configured partition count; with AutoCreate off it instead
// validates that they already exist and fails fast, the enterprise pattern where
// topic creation is managed out of band.
package admin

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Config configures topic provisioning. Shared by origin, retry-tier, and DLQ
// topics.
type Config struct {
	// AutoCreate, when true, creates missing topics on Start; when false, Start
	// validates they exist and fails fast. Default true.
	AutoCreate bool `mapstructure:"auto_create"`
	// Partitions is the partition count for created topics. Default 4.
	Partitions int32 `mapstructure:"partitions"`
	// ReplicationFactor is the replication factor for created topics. Default 1
	// (set >= 3 in production).
	ReplicationFactor int16 `mapstructure:"replication_factor"`
	// Configs are extra per-topic Kafka configs (e.g. retention.ms) applied at
	// creation.
	Configs map[string]string `mapstructure:"configs"`
}

// DefaultConfig returns provisioning defaults.
func DefaultConfig() Config {
	return Config{AutoCreate: true, Partitions: 4, ReplicationFactor: 1}
}

// ApplyDefaults fills non-positive partition/replication values.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.Partitions <= 0 {
		c.Partitions = d.Partitions
	}
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = d.ReplicationFactor
	}
}

// Provision creates (AutoCreate) or validates (otherwise) the given topics.
func Provision(ctx context.Context, cl *kgo.Client, cfg Config, topics []string, logger *log.Logger) error {
	if cfg.AutoCreate {
		return EnsureTopics(ctx, cl, cfg, topics, logger)
	}
	return ValidateTopics(ctx, cl, topics)
}

// EnsureTopics idempotently creates the given topics at the configured partition
// count and replication factor. An already-existing topic is not an error.
func EnsureTopics(ctx context.Context, cl *kgo.Client, cfg Config, topics []string, logger *log.Logger) error {
	topics = unique(topics)
	if len(topics) == 0 {
		return nil
	}
	cfg.ApplyDefaults()
	if logger == nil {
		logger = log.Nop()
	}

	configs := make(map[string]*string, len(cfg.Configs))
	for k, v := range cfg.Configs {
		v := v
		configs[k] = &v
	}

	adm := kadm.NewClient(cl)
	resps, err := adm.CreateTopics(ctx, cfg.Partitions, cfg.ReplicationFactor, configs, topics...)
	if err != nil {
		return fmt.Errorf("kafka: create topics: %w", err)
	}
	for _, r := range resps.Sorted() {
		switch {
		case r.Err == nil:
			logger.Info("kafka topic created",
				zap.String("topic", r.Topic),
				zap.Int32("partitions", cfg.Partitions))
		case errors.Is(r.Err, kerr.TopicAlreadyExists):
			// idempotent — already provisioned
		default:
			return fmt.Errorf("kafka: create topic %q: %w", r.Topic, r.Err)
		}
	}
	return nil
}

// ValidateTopics returns an error listing any of the given topics that do not
// exist, so a service with AutoCreate off fails fast at startup.
func ValidateTopics(ctx context.Context, cl *kgo.Client, topics []string) error {
	topics = unique(topics)
	if len(topics) == 0 {
		return nil
	}
	adm := kadm.NewClient(cl)
	details, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return fmt.Errorf("kafka: list topics: %w", err)
	}
	var missing []string
	for _, t := range topics {
		d, ok := details[t]
		if !ok || (d.Err != nil && errors.Is(d.Err, kerr.UnknownTopicOrPartition)) {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("kafka: required topics do not exist (auto_create is off): %v", missing)
	}
	return nil
}

func unique(in []string) []string {
	if len(in) == 0 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
