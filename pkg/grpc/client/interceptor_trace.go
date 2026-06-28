package client

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ensureIDsUnary ensures a span context (and thus a trace id) is present on the
// outgoing context. Used on the tracing-OFF path so the correlation interceptor
// has a stable id to emit.
func ensureIDsUnary() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(ensureIDsCtx(ctx), method, req, reply, cc, opts...)
	}
}

// ensureIDsStream is the streaming counterpart of ensureIDsUnary.
func ensureIDsStream() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(ensureIDsCtx(ctx), desc, cc, method, opts...)
	}
}

// requestTimeoutUnary applies d as the call deadline when the caller's context
// has none. Streaming RPCs are intentionally not bounded (they may be long-lived).
func requestTimeoutUnary(d time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if d > 0 {
			if _, ok := ctx.Deadline(); !ok {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, d)
				defer cancel()
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// correlationUnary injects the custom trace-id metadata header from the span
// context (tracing-OFF path). It does NOT emit a W3C traceparent.
func correlationUnary(cfg TraceConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(injectCorrelation(ctx, cfg), method, req, reply, cc, opts...)
	}
}

// correlationStream is the streaming counterpart of correlationUnary.
func correlationStream(cfg TraceConfig) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(injectCorrelation(ctx, cfg), desc, cc, method, opts...)
	}
}

// injectCorrelation appends the custom trace-id header to the outgoing metadata
// from the ensured span context's trace id.
func injectCorrelation(ctx context.Context, cfg TraceConfig) context.Context {
	if !boolValue(cfg.Propagate) || cfg.TraceIDHeader == "" {
		return ctx
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, cfg.TraceIDHeader, sc.TraceID().String())
}
