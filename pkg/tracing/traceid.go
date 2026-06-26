package tracing

import (
	"context"
	"crypto/rand"

	"go.opentelemetry.io/otel/trace"
)

// GenerateTraceID returns a random, valid W3C trace id (16 bytes). It guards
// against the (astronomically rare) all-zero id, which OTel rejects as invalid.
func GenerateTraceID() trace.TraceID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	t := trace.TraceID(b)
	if !t.IsValid() {
		b[15] |= 0x01
		t = trace.TraceID(b)
	}
	return t
}

// GenerateSpanID returns a random, valid span id (8 bytes).
func GenerateSpanID() trace.SpanID {
	var b [8]byte
	_, _ = rand.Read(b[:])
	s := trace.SpanID(b)
	if !s.IsValid() {
		b[7] |= 0x01
		s = trace.SpanID(b)
	}
	return s
}

// WithGeneratedSpanContext ensures ctx carries a valid OTel span context: if one
// is already present it is returned unchanged; otherwise a freshly generated,
// sampled span context is attached. This is used by background workers (e.g.
// cron jobs) that have no inbound request so logs and propagation always have a
// trace id, even when tracing export is disabled.
func WithGeneratedSpanContext(ctx context.Context) context.Context {
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    GenerateTraceID(),
		SpanID:     GenerateSpanID(),
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}
