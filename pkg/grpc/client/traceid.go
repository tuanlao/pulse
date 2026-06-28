package client

import (
	"context"

	"github.com/tuanlao/pulse/pkg/tracing"
	"go.opentelemetry.io/otel/trace"
)

// ensureIDsCtx guarantees ctx carries a valid OTel span context, generating one
// when absent. On the tracing-OFF path this gives a stable trace id for the
// x-trace-id correlation header and for log correlation.
//
// The generated span context is attached locally (NOT as a remote parent): with
// no real tracer there is no upstream span, so emitting a W3C traceparent would
// falsely tell the server an upstream traced span exists. Hence the tracing-OFF
// path only propagates the custom x-trace-id header, never traceparent.
func ensureIDsCtx(ctx context.Context) context.Context {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		return ctx
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tracing.GenerateTraceID(),
		SpanID:     tracing.GenerateSpanID(),
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}
