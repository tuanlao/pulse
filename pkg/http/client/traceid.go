package client

import (
	"context"

	"github.com/tuanlao/pulse/pkg/tracing"
	"go.opentelemetry.io/otel/trace"
)

// ensureIDs guarantees that ctx carries a valid OTel span context (a trace id +
// span id), generating one when absent. This runs at the top of every call so
// every downstream RoundTripper — otelhttp (tracing on) or the manual injector
// (tracing off) — has a stable trace id to emit.
//
// A freshly generated span context is attached as a REMOTE parent so that, when
// a real TracerProvider is present, otelhttp parents its client span to this
// trace id (preserving the trace id while assigning a fresh child span id).
func ensureIDs(ctx context.Context) context.Context {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return ctx
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tracing.GenerateTraceID(),
		SpanID:     tracing.GenerateSpanID(),
		TraceFlags: trace.FlagsSampled,
		Remote:     false,
	})
	// Attach as a REMOTE parent so otelhttp (tracing on) parents its client
	// span to this trace id while assigning a fresh child span id.
	return trace.ContextWithRemoteSpanContext(ctx, sc)
}

// traceparent renders the W3C traceparent header value for sc.
func traceparent(sc trace.SpanContext) string {
	flags := "00"
	if sc.IsSampled() {
		flags = "01"
	}
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-" + flags
}
