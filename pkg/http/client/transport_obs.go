package client

import (
	"net/http"
	"strconv"
	"time"

	"github.com/tuanlao/pulse/pkg/log"
	"go.uber.org/zap"
)

// obsRT records metrics and a log line for the whole logical call. It is the
// outermost decorator (above retry), so latency is total wall time and each
// counter increments once per call, not once per attempt.
type obsRT struct {
	next    http.RoundTripper
	metrics *ClientMetrics // optional
	logger  *log.Logger    // base/fallback logger; may be nil
}

func (rt *obsRT) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := rt.next.RoundTrip(req)
	dur := time.Since(start)

	host := req.URL.Hostname() // host only — bounded cardinality
	method := req.Method
	status := 0
	if resp != nil {
		status = resp.StatusCode
	}

	// A done request context means the caller canceled or the deadline fired —
	// that is client-side, not an upstream failure, so keep it out of the error
	// metric and warn logs.
	canceled := err != nil && req.Context().Err() != nil

	if rt.metrics != nil {
		rt.metrics.observe(host, method, statusLabel(status, err, canceled), dur)
	}

	if rt.logger != nil {
		// Derive a logger from the base + current context once: ForContext attaches
		// trace_id/span_id from the ensured span context. (Don't combine with
		// FromContext — that would inherit the inbound request-scoped logger.)
		l := rt.logger.ForContext(req.Context())
		fields := []zap.Field{
			zap.String("method", method),
			zap.String("url", req.URL.String()),
			zap.Int("status", status),
			zap.Duration("latency", dur),
		}
		switch {
		case canceled:
			l.Info("outbound request canceled", append(fields, zap.Error(err))...)
		case err != nil:
			l.Warn("outbound request failed", append(fields, zap.Error(err))...)
		default:
			l.Info("outbound request", fields...)
		}
	}
	return resp, err
}

// statusLabel maps a status/error/canceled tuple to a bounded metric label.
func statusLabel(status int, err error, canceled bool) string {
	switch {
	case canceled:
		return "canceled"
	case err != nil:
		return "error"
	case status == 0:
		return "0"
	default:
		return strconv.Itoa(status)
	}
}
