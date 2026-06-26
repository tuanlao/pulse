package client

import (
	"context"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxDrainBytes bounds how much of a discarded (retried) response body we read to
// allow keep-alive connection reuse. Larger or unknown-length bodies are left
// unread and the connection is dropped instead — mirrors net/http's own
// conservative drain (maxBodySlurpSize).
const maxDrainBytes = 64 << 10

// retryRT retries idempotent requests on retryable statuses / transport errors,
// with exponential backoff + optional full jitter. It sits ABOVE the trace layer
// so each attempt gets its own client span; the obs layer above it records a
// single metric/log for the whole logical call.
type retryRT struct {
	next    http.RoundTripper
	cfg     RetryConfig
	metrics *ClientMetrics // optional, for retries_total
}

func (rt *retryRT) RoundTrip(req *http.Request) (*http.Response, error) {
	if !boolValue(rt.cfg.Enabled) || reqNoRetry(req.Context()) || !rt.methodRetryable(req.Method) {
		return rt.next.RoundTrip(req)
	}
	// A retried request must be replayable. If the body can't be rewound, do a
	// single attempt rather than risk sending a half-consumed body.
	if req.Body != nil && req.Body != http.NoBody && req.GetBody == nil {
		return rt.next.RoundTrip(req)
	}

	var lastResp *http.Response
	var lastErr error
	for attempt := 1; attempt <= rt.cfg.MaxAttempts; attempt++ {
		attemptReq := req.Clone(req.Context())
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, err
			}
			attemptReq.Body = body
		}

		resp, err := rt.next.RoundTrip(attemptReq)
		lastResp, lastErr = resp, err

		if !rt.shouldRetry(req.Context(), resp, err) {
			return resp, err
		}
		if attempt == rt.cfg.MaxAttempts {
			break
		}

		// Drain+close the discarded response so the connection can return to the
		// pool. Only drain when the body is known and small: a fully-drained body
		// lets net/http reuse the keep-alive connection, but reading a large (or
		// unknown-length) 5xx error body just to save one connection is a poor
		// trade and a DoS vector, so for those we skip the read and just close.
		if resp != nil {
			if resp.ContentLength >= 0 && resp.ContentLength <= maxDrainBytes {
				_, _ = io.Copy(io.Discard, resp.Body)
			}
			_ = resp.Body.Close()
		}
		if rt.metrics != nil {
			rt.metrics.addRetry(req.URL.Hostname(), req.Method)
		}

		wait := rt.backoff(attempt, resp)
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		}
	}
	return lastResp, lastErr
}

func (rt *retryRT) methodRetryable(method string) bool {
	for _, m := range rt.cfg.Methods {
		if strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

// shouldRetry decides whether to retry. A done call context (caller cancel or
// overall deadline) is terminal; otherwise a transport error or a retryable
// status is retried.
func (rt *retryRT) shouldRetry(ctx context.Context, resp *http.Response, err error) bool {
	if ctx.Err() != nil {
		return false
	}
	if err != nil {
		return true
	}
	if resp == nil {
		return false
	}
	for _, s := range rt.cfg.RetryStatuses {
		if resp.StatusCode == s {
			return true
		}
	}
	return false
}

// backoff returns the wait before the next attempt: exponential, capped, with
// optional full jitter; honoring Retry-After when configured.
func (rt *retryRT) backoff(attempt int, resp *http.Response) time.Duration {
	if boolValue(rt.cfg.RespectRetryAfter) && resp != nil {
		if d, ok := retryAfter(resp); ok {
			if d > rt.cfg.MaxBackoff {
				d = rt.cfg.MaxBackoff
			}
			return d
		}
	}
	// base * 2^(attempt-1), capped.
	wait := rt.cfg.BaseBackoff << (attempt - 1)
	if wait <= 0 || wait > rt.cfg.MaxBackoff {
		wait = rt.cfg.MaxBackoff
	}
	if rt.cfg.Jitter && wait > 0 {
		wait = time.Duration(rand.Int64N(int64(wait) + 1))
	}
	return wait
}

// retryAfter parses the Retry-After header (delta-seconds or HTTP-date).
func retryAfter(resp *http.Response) (time.Duration, bool) {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
