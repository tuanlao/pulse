// Package kafka is the facade of pulse's Kafka support. It composes a set of
// focused sub-packages — producer, consumer, retry (Spring-style non-blocking
// retry topics + DLQ), dedup (otter/redis), admin (topic provisioning), metrics,
// trace, codec — behind two constructors, NewProducer and NewConsumer, plus the
// generic Send/On helpers. Keeping each subsystem in its own package keeps this
// library maintainable; the facade just wires them.
//
// Public types from the leaf message/codec/retry packages are re-exported here as
// aliases so callers import only "pkg/kafka".
package kafka

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"
	"github.com/tuanlao/pulse/pkg/kafka/codec"
	"github.com/tuanlao/pulse/pkg/kafka/consumer"
	"github.com/tuanlao/pulse/pkg/kafka/internal/kclient"
	"github.com/tuanlao/pulse/pkg/kafka/message"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	"github.com/tuanlao/pulse/pkg/kafka/producer"
	"github.com/tuanlao/pulse/pkg/kafka/retry"
	"github.com/tuanlao/pulse/pkg/log"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Re-exported public types so callers depend only on this package.
type (
	// Message is the record envelope a handler receives / a producer sends.
	Message = message.Message
	// Handler processes one message.
	Handler = message.Handler
	// Headers is the typed header accessor.
	Headers = message.Headers
	// Hooks are the optional lifecycle observers.
	Hooks = message.Hooks
	// ErrorClass categorizes a failure.
	ErrorClass = message.ErrorClass
	// Codec (de)serializes typed event values.
	Codec = codec.Codec
	// ClientConfig is the Kafka connection config.
	ClientConfig = kclient.Config
	// Producer publishes records.
	Producer = producer.Producer
	// Consumer consumes a group's topics.
	Consumer = consumer.Consumer
	// Metrics holds the package-owned Prometheus collectors.
	Metrics = kmetrics.Metrics
	// ForwardResult describes a retry/DLQ forward outcome.
	ForwardResult = retry.ForwardResult
	// ReplayPolicy decides whether a DLQ'd class is replayable.
	ReplayPolicy = retry.ReplayPolicy
)

// Re-exported error-class constants.
const (
	ErrorNonRetryable     = message.ErrorNonRetryable
	ErrorRetriesExhausted = message.ErrorRetriesExhausted
)

// Re-exported helpers.
var (
	// NonRetryable wraps an error so the message goes straight to the DLQ.
	NonRetryable = message.NonRetryable
	// IsNonRetryable reports whether an error was marked NonRetryable.
	IsNonRetryable = message.IsNonRetryable
	// NewMessage builds a message for producing (with a fresh id).
	NewMessage = message.NewMessage
	// Replayable reports whether a class is replayable under the default policy.
	Replayable = retry.Replayable
	// JSONCodec is the default codec.
	JSONCodec = codec.JSON
)

// Deps are the facade's collaborators. All are nil-safe (degrade to no-op).
type Deps struct {
	// Logger logs lifecycle + processing events; nil -> no-op.
	Logger *log.Logger
	// TracerProvider creates produce/consume spans; nil -> no-op.
	TracerProvider oteltrace.TracerProvider
	// Metrics is built with NewMetrics (sharing the server registry); nil disables.
	Metrics *kmetrics.Metrics
	// RedisClient is the shared rueidis client for the global (redis) deduper. Not
	// owned (not closed by kafka).
	RedisClient rueidis.Client
	// Codec overrides the default JSON codec for Send/On.
	Codec codec.Codec
	// Hooks observe the message lifecycle.
	Hooks message.Hooks
}

// NewMetrics builds the package-owned Prometheus metrics (registering into reg).
// It returns nil when cfg.Enabled is false.
func NewMetrics(cfg kmetrics.Config, reg *prometheus.Registry) (*kmetrics.Metrics, error) {
	return kmetrics.NewMetrics(cfg, reg)
}

// NewProducer builds the Kafka producer. When the component is disabled it
// returns a no-op producer. The wiring lives in builder.go.
func NewProducer(cfg Config, deps Deps, opts ...Option) (*producer.Producer, error) {
	cfg = resolve(cfg, opts)
	if !cfg.Enabled {
		return producer.Disabled(deps.Logger), nil
	}
	return producer.New(cfg.Producer, buildProducerDeps(cfg, deps))
}

// NewConsumer builds the Kafka consumer. When the component is disabled it
// returns a no-op consumer. Bind handlers with consumer.Register / On before
// registering it into the lifecycle manager.
func NewConsumer(cfg Config, deps Deps, opts ...Option) (*consumer.Consumer, error) {
	cfg = resolve(cfg, opts)
	if !cfg.Enabled {
		return consumer.Disabled(deps.Logger), nil
	}
	cdeps, err := buildConsumerDeps(cfg, deps)
	if err != nil {
		return nil, err
	}
	return consumer.New(cfg.Consumer, cdeps)
}

// Send marshals a typed event with the producer's codec and produces it to topic.
func Send[T any](ctx context.Context, p *producer.Producer, topic, key string, e T) error {
	return p.Send(ctx, topic, key, e)
}

// On registers a typed handler: the record payload is decoded into T with the
// consumer's codec before the handler runs. A decode failure is non-retryable
// (it routes straight to the DLQ). The raw Register is still available for byte
// handlers.
func On[T any](c *consumer.Consumer, topic string, h func(ctx context.Context, e T, m *Message) error) {
	cd := c.Codec()
	c.Register(topic, func(ctx context.Context, m *Message) error {
		var e T
		if err := cd.Unmarshal(m.Value, &e); err != nil {
			return message.NonRetryable(fmt.Errorf("kafka: decode %T: %w", e, err))
		}
		return h(ctx, e, m)
	})
}

// DLQTopic returns the DLQ topic name for an origin under cfg (so a service can
// Register a handler to consume it).
func DLQTopic(cfg Config, origin string) string {
	cfg.applyDefaults()
	return retry.NewNamer(cfg.Retry).DLQTopic(origin, cfg.Consumer.GroupID)
}
