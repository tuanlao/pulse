package kafka

import (
	"github.com/tuanlao/pulse/pkg/kafka/codec"
	"github.com/tuanlao/pulse/pkg/kafka/consumer"
	"github.com/tuanlao/pulse/pkg/kafka/dedup"
	"github.com/tuanlao/pulse/pkg/kafka/producer"
)

// resolve applies defaults, then the options, then defaults again (the pulse
// constructor convention) and returns the resolved config.
func resolve(cfg Config, opts []Option) Config {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()
	return cfg
}

// buildProducerDeps assembles the producer's collaborators from the facade deps.
func buildProducerDeps(cfg Config, deps Deps) producer.Deps {
	return producer.Deps{
		Conn:           cfg.Client,
		Admin:          cfg.Admin,
		Tracing:        cfg.Trace,
		Codec:          codec.Or(deps.Codec),
		Metrics:        deps.Metrics,
		Logger:         deps.Logger,
		TracerProvider: deps.TracerProvider,
		Hooks:          deps.Hooks,
		Source:         cfg.source(),
	}
}

// buildConsumerDeps assembles the consumer's collaborators, building the deduper
// (which may need the shared redis client).
func buildConsumerDeps(cfg Config, deps Deps) (consumer.Deps, error) {
	// A disabled pulse *redis.Client is a non-nil interface wrapping a nil rueidis
	// client (it never dialed); treat it as absent so redis-mode dedup fails fast
	// (or local-mode ignores it) instead of panicking on first command.
	redisClient := deps.RedisClient
	if e, ok := redisClient.(interface{ Enabled() bool }); ok && !e.Enabled() {
		redisClient = nil
	}
	deduper, err := dedup.New(cfg.Dedup, redisClient)
	if err != nil {
		return consumer.Deps{}, err
	}
	return consumer.Deps{
		Conn:           cfg.Client,
		Admin:          cfg.Admin,
		Retry:          cfg.Retry,
		Tracing:        cfg.Trace,
		DedupTTL:       cfg.Dedup.TTL,
		Codec:          codec.Or(deps.Codec),
		Deduper:        deduper,
		Metrics:        deps.Metrics,
		Logger:         deps.Logger,
		TracerProvider: deps.TracerProvider,
		Hooks:          deps.Hooks,
	}, nil
}
