package client

import (
	"net/http"

	"go.opentelemetry.io/otel/trace"
)

// headerRT injects the custom trace-id header. It is used on the tracing-ON
// path, wrapping otelhttp: otelhttp owns the W3C traceparent + the client span;
// headerRT only adds the custom header. It sets X-Trace-Id from the TRACE id
// (stable across parent/child), not the span id, so the value is identical
// whether tracing is on or off.
type headerRT struct {
	next http.RoundTripper
	cfg  TraceConfig
}

func (rt *headerRT) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	if boolValue(rt.cfg.Propagate) && rt.cfg.TraceIDHeader != "" {
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			req.Header.Set(rt.cfg.TraceIDHeader, sc.TraceID().String())
		}
	}
	return rt.next.RoundTrip(req)
}

// manualTraceRT injects the W3C traceparent and the custom trace-id header
// directly from the span context already ensured on the request. It is used on
// the tracing-OFF path (no real provider), where otelhttp would otherwise
// produce an invalid span context.
type manualTraceRT struct {
	next http.RoundTripper
	cfg  TraceConfig
}

func (rt *manualTraceRT) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		if boolValue(rt.cfg.InjectTraceparent) {
			req.Header.Set("Traceparent", traceparent(sc))
		}
		if boolValue(rt.cfg.Propagate) && rt.cfg.TraceIDHeader != "" {
			req.Header.Set(rt.cfg.TraceIDHeader, sc.TraceID().String())
		}
	}
	return rt.next.RoundTrip(req)
}
