// Package producer is the Kafka producer component (lifecycle.Component). It
// produces records (async Produce + promise, or ProduceSync) and offers a typed
// Send that marshals an event via the configured codec. Topics are provisioned on
// Start (auto-create or validate). It stamps the standard headers (message id,
// source, content type) and injects the W3C trace context.
package producer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tuanlao/pulse/pkg/kafka/admin"
	"github.com/tuanlao/pulse/pkg/kafka/codec"
	"github.com/tuanlao/pulse/pkg/kafka/internal/kclient"
	"github.com/tuanlao/pulse/pkg/kafka/message"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	ktrace "github.com/tuanlao/pulse/pkg/kafka/trace"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Config is the producer behavior configuration.
type Config struct {
	// RequiredAcks is "all" (default), "leader", or "none". Non-"all" disables
	// idempotent writes.
	RequiredAcks string `mapstructure:"required_acks"`
	// Linger batches records for up to this long before sending. Default 10ms.
	Linger time.Duration `mapstructure:"linger"`
	// MaxBufferedRecords caps in-flight buffered records (0 = kgo default).
	MaxBufferedRecords int `mapstructure:"max_buffered_records"`
	// Compression is "snappy" (default), "gzip", "lz4", "zstd", or "none".
	Compression string `mapstructure:"compression"`
	// FlushTimeout bounds the flush on Stop. Default 30s.
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`
	// Topics are provisioned on Start (auto-created or validated).
	Topics []string `mapstructure:"topics"`
}

// DefaultConfig returns producer defaults.
func DefaultConfig() Config {
	return Config{
		RequiredAcks: "all",
		Linger:       10 * time.Millisecond,
		Compression:  "snappy",
		FlushTimeout: 30 * time.Second,
	}
}

// ApplyDefaults fills empty fields.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.RequiredAcks == "" {
		c.RequiredAcks = d.RequiredAcks
	}
	if c.Compression == "" {
		c.Compression = d.Compression
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = d.FlushTimeout
	}
}

// Deps are the producer's collaborators (built by the facade).
type Deps struct {
	Conn           kclient.Config
	Admin          admin.Config
	Tracing        ktrace.Config
	Codec          codec.Codec
	Metrics        *kmetrics.Metrics
	Logger         *log.Logger
	TracerProvider trace.TracerProvider
	Hooks          message.Hooks
	// Source identifies the producing service (stamped as x-source).
	Source string
}

// Producer publishes records to Kafka. It implements lifecycle.Component.
type Producer struct {
	cfg      Config
	deps     Deps
	cl       *kgo.Client
	codec    codec.Codec
	tracer   trace.Tracer
	log      *log.Logger
	disabled bool
}

// New builds a Producer: it constructs the kgo client from the connection config
// plus the producer tuning options.
func New(cfg Config, deps Deps) (*Producer, error) {
	cfg.ApplyDefaults()
	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}

	opts, err := producerOpts(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kclient.Build(deps.Conn, opts...)
	if err != nil {
		return nil, err
	}

	return &Producer{
		cfg:    cfg,
		deps:   deps,
		cl:     cl,
		codec:  codec.Or(deps.Codec),
		tracer: ktrace.Tracer(deps.TracerProvider, deps.Tracing),
		log:    deps.Logger,
	}, nil
}

// Disabled returns a no-op producer (used when the kafka component is disabled),
// so a service can always register it safely.
func Disabled(logger *log.Logger) *Producer {
	if logger == nil {
		logger = log.Nop()
	}
	return &Producer{codec: codec.JSON, log: logger, disabled: true}
}

// Client returns the underlying kgo client (escape hatch / tests).
func (p *Producer) Client() *kgo.Client { return p.cl }

// Codec returns the producer's codec.
func (p *Producer) Codec() codec.Codec { return p.codec }

// Name implements lifecycle.Component.
func (p *Producer) Name() string { return "kafka-producer" }

// Start provisions the configured topics (auto-create or validate). Non-blocking.
func (p *Producer) Start(ctx context.Context) error {
	if p.disabled {
		return nil
	}
	if err := p.cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka producer: ping: %w", err)
	}
	return admin.Provision(ctx, p.cl, p.deps.Admin, p.cfg.Topics, p.log)
}

// Stop flushes buffered records (bounded by FlushTimeout) then closes the client.
func (p *Producer) Stop(ctx context.Context) error {
	if p.disabled || p.cl == nil {
		return nil
	}
	fctx, cancel := context.WithTimeout(ctx, p.cfg.FlushTimeout)
	defer cancel()
	if err := p.cl.Flush(fctx); err != nil {
		p.cl.Close()
		return fmt.Errorf("kafka producer: flush: %w", err)
	}
	p.cl.Close()
	return nil
}

// Send marshals event with the codec and produces it to topic under key.
func (p *Producer) Send(ctx context.Context, topic, key string, event any) error {
	if p.disabled {
		return nil
	}
	data, err := p.codec.Marshal(event)
	if err != nil {
		return fmt.Errorf("kafka producer: marshal: %w", err)
	}
	var k []byte
	if key != "" {
		k = []byte(key)
	}
	m := message.NewMessage(k, data)
	m.Headers.SetContentType(p.codec.ContentType())
	return p.ProduceSync(ctx, topic, m)
}

// Produce sends a message asynchronously; errors are reported via the promise
// (OnProduceError hook + log), never returned.
func (p *Producer) Produce(ctx context.Context, topic string, m *message.Message) {
	if p.disabled {
		return
	}
	p.stamp(m)
	ctx, span := ktrace.StartProduceSpan(ctx, p.tracer, topic)
	rec := m.ToRecord(topic)
	ktrace.Inject(ctx, rec)
	start := time.Now()
	p.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		defer span.End()
		if err != nil {
			span.RecordError(err)
			p.deps.Metrics.ObserveProduce(topic, "error", time.Since(start))
			p.deps.Hooks.FireProduceError(ctx, m, err)
			p.log.ForContext(ctx).Error("kafka produce failed", zap.String("topic", topic), zap.Error(err))
			return
		}
		p.deps.Metrics.ObserveProduce(topic, "ok", time.Since(start))
		p.deps.Hooks.FireProduce(ctx, m)
	})
}

// ProduceSync sends a message synchronously, returning the produce error.
func (p *Producer) ProduceSync(ctx context.Context, topic string, m *message.Message) error {
	if p.disabled {
		return nil
	}
	p.stamp(m)
	ctx, span := ktrace.StartProduceSpan(ctx, p.tracer, topic)
	defer span.End()
	rec := m.ToRecord(topic)
	ktrace.Inject(ctx, rec)
	start := time.Now()
	err := p.cl.ProduceSync(ctx, rec).FirstErr()
	status := "ok"
	if err != nil {
		status = "error"
		span.RecordError(err)
	}
	p.deps.Metrics.ObserveProduce(topic, status, time.Since(start))
	if err != nil {
		p.deps.Hooks.FireProduceError(ctx, m, err)
		return fmt.Errorf("kafka producer: produce to %q: %w", topic, err)
	}
	p.deps.Hooks.FireProduce(ctx, m)
	return nil
}

// stamp fills the standard headers that may be missing on a hand-built message.
func (p *Producer) stamp(m *message.Message) {
	if m.Headers.MessageID() == "" {
		m.Headers.SetMessageID(message.NewID())
	}
	if p.deps.Source != "" && m.Headers.Source() == "" {
		m.Headers.SetSource(p.deps.Source)
	}
}

// producerOpts maps Config to kgo producer options.
func producerOpts(cfg Config) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	switch strings.ToLower(strings.TrimSpace(cfg.RequiredAcks)) {
	case "", "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader", "one", "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case "none", "0":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	default:
		return nil, fmt.Errorf("kafka producer: unknown required_acks %q", cfg.RequiredAcks)
	}
	if cfg.Linger > 0 {
		opts = append(opts, kgo.ProducerLinger(cfg.Linger))
	}
	if cfg.MaxBufferedRecords > 0 {
		opts = append(opts, kgo.MaxBufferedRecords(cfg.MaxBufferedRecords))
	}
	c, err := compression(cfg.Compression)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ProducerBatchCompression(c))
	return opts, nil
}

func compression(name string) (kgo.CompressionCodec, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "snappy":
		return kgo.SnappyCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	default:
		return kgo.NoCompression(), fmt.Errorf("kafka producer: unknown compression %q", name)
	}
}
