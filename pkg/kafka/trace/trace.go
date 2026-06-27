// Package trace wires Kafka records into OpenTelemetry: it propagates the W3C
// trace context through record headers (so a trace flows producer -> consumer
// across services) and starts producer/consumer spans. It is hand-rolled (a tiny
// TextMapCarrier over kgo record headers) rather than pulling in a heavier
// plugin, matching pulse's style and reusing the same propagation.TraceContext
// the HTTP layer uses.
package trace

import (
	"context"

	"github.com/tuanlao/pulse/pkg/tracing"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config toggles tracing for the kafka component. Kafka consumes a
// TracerProvider via Deps, so it only needs an on/off switch here (like the
// redis and cron components).
type Config struct {
	// Enabled toggles span creation and header propagation. Default true.
	Enabled bool `mapstructure:"enabled"`
}

// DefaultConfig returns the tracing defaults.
func DefaultConfig() Config { return Config{Enabled: true} }

// propagator is the W3C trace-context propagator (traceparent / tracestate).
var propagator = propagation.TraceContext{}

// Tracer builds a tracer named for this package. When cfg.Enabled is false (or
// tp is nil) it returns a no-op tracer so callers never branch on nil.
func Tracer(tp trace.TracerProvider, cfg Config) trace.Tracer {
	if !cfg.Enabled || tp == nil {
		tp = noop.NewTracerProvider()
	}
	return tp.Tracer("github.com/tuanlao/pulse/pkg/kafka")
}

// recordCarrier adapts a *kgo.Record's headers to a TextMapCarrier so the W3C
// propagator can read/write traceparent + tracestate on it.
type recordCarrier struct{ r *kgo.Record }

func (c recordCarrier) Get(key string) string {
	for _, h := range c.r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c recordCarrier) Set(key, value string) {
	for i := range c.r.Headers {
		if c.r.Headers[i].Key == key {
			c.r.Headers[i].Value = []byte(value)
			return
		}
	}
	c.r.Headers = append(c.r.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c recordCarrier) Keys() []string {
	keys := make([]string, 0, len(c.r.Headers))
	for _, h := range c.r.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// Inject writes the active span context from ctx into the record's headers.
func Inject(ctx context.Context, r *kgo.Record) {
	propagator.Inject(ctx, recordCarrier{r: r})
}

// Extract returns a context carrying the trace context found in the record's
// headers. When none is present it synthesizes a valid span context (via
// tracing.WithGeneratedSpanContext) so handler logs always carry a trace id,
// even with tracing export disabled.
func Extract(ctx context.Context, r *kgo.Record) context.Context {
	ctx = propagator.Extract(ctx, recordCarrier{r: r})
	return tracing.WithGeneratedSpanContext(ctx)
}

// StartProduceSpan starts a producer-kind span named "kafka.produce <topic>".
func StartProduceSpan(ctx context.Context, t trace.Tracer, topic string) (context.Context, trace.Span) {
	return t.Start(ctx, "kafka.produce "+topic, trace.WithSpanKind(trace.SpanKindProducer))
}

// StartConsumeSpan starts a consumer-kind span named "kafka.consume <topic>".
func StartConsumeSpan(ctx context.Context, t trace.Tracer, topic string) (context.Context, trace.Span) {
	return t.Start(ctx, "kafka.consume "+topic, trace.WithSpanKind(trace.SpanKindConsumer))
}
