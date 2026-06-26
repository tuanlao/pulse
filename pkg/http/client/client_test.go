package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recorder captures inbound request data from the test server.
type recorder struct {
	mu     sync.Mutex
	hits   int
	header http.Header
	bodies []string
}

func (r *recorder) record(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hits++
	r.header = req.Header.Clone()
	b, _ := io.ReadAll(req.Body)
	r.bodies = append(r.bodies, string(b))
}

func (r *recorder) snapshot() (int, http.Header, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hits, r.header, append([]string(nil), r.bodies...)
}

var traceparentRe = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`)

func newClient(t *testing.T, base string, deps Deps, opts ...Option) *Client {
	t.Helper()
	c, err := New(DefaultConfig(), deps, append([]Option{WithBaseURL(base)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestTraceID_AlwaysPresent_TracingOff(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newClient(t, srv.URL, Deps{}) // no tracer provider -> manual path
	resp, err := c.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/x"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	_, h, _ := rec.snapshot()
	tp := h.Get("Traceparent")
	xt := h.Get("X-Trace-Id")
	if !traceparentRe.MatchString(tp) {
		t.Fatalf("traceparent %q does not match %s", tp, traceparentRe)
	}
	if xt == "" {
		t.Fatalf("X-Trace-Id missing")
	}
	// trace id portion of traceparent must equal X-Trace-Id
	if got := strings.Split(tp, "-")[1]; got != xt {
		t.Fatalf("traceparent trace id %q != X-Trace-Id %q", got, xt)
	}
}

func TestTraceID_AlwaysPresent_TracingOn(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr), sdktrace.WithSampler(sdktrace.AlwaysSample()))

	c := newClient(t, srv.URL, Deps{TracerProvider: tp})
	if !c.tracing {
		t.Fatalf("expected client to detect a real provider")
	}
	resp, err := c.Do(context.Background(), &Request{Method: http.MethodGet, Path: "/x"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	_, h, _ := rec.snapshot()
	xt := h.Get("X-Trace-Id")
	tpHeader := h.Get("Traceparent")
	if xt == "" || !traceparentRe.MatchString(tpHeader) {
		t.Fatalf("missing trace headers: X-Trace-Id=%q traceparent=%q", xt, tpHeader)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 client span, got %d", len(spans))
	}
	span := spans[0]
	if span.SpanContext().TraceID().String() != xt {
		t.Fatalf("span trace id %s != X-Trace-Id %s", span.SpanContext().TraceID(), xt)
	}
	if got := strings.Split(tpHeader, "-")[1]; got != xt {
		t.Fatalf("traceparent trace id %q != X-Trace-Id %q", got, xt)
	}
}

func TestTraceID_PropagatesIncomingTrace(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled, Remote: true})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)

	c := newClient(t, srv.URL, Deps{}) // tracing off; should still reuse incoming trace id
	resp, err := c.Do(ctx, &Request{Method: http.MethodGet, Path: "/x"})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	_, h, _ := rec.snapshot()
	if h.Get("X-Trace-Id") != tid.String() {
		t.Fatalf("expected reused trace id %s, got %s", tid, h.Get("X-Trace-Id"))
	}
	if got := strings.Split(h.Get("Traceparent"), "-")[1]; got != tid.String() {
		t.Fatalf("traceparent did not reuse trace id: %s", got)
	}
}

func TestTraceID_GeneratedWhenAbsent(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := newClient(t, srv.URL, Deps{}) // tracing off, no incoming trace

	// The client always emits a trace id, generating one when the context has
	// none — so X-Trace-Id and the traceparent are present and consistent.
	resp, _ := c.Do(context.Background(), &Request{Path: "/a"})
	resp.Body.Close()
	_, h, _ := rec.snapshot()

	xt := h.Get("X-Trace-Id")
	if xt == "" {
		t.Fatalf("expected a generated X-Trace-Id header")
	}
	if got := strings.Split(h.Get("Traceparent"), "-")[1]; got != xt {
		t.Fatalf("traceparent trace id %q != X-Trace-Id %q", got, xt)
	}
}

func TestRetry_ReplaysBodyAndStops(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if rec.hits < 3 { // first two attempts fail
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := newClient(t, srv.URL, Deps{}, WithRetry(fastRetry()))
	var out map[string]bool
	// PUT is idempotent -> retried by default.
	if err := c.PutJSON(context.Background(), "/x", map[string]int{"n": 1}, &out); err != nil {
		t.Fatalf("PutJSON: %v", err)
	}
	hits, _, bodies := rec.snapshot()
	if hits != 3 {
		t.Fatalf("expected 3 attempts, got %d", hits)
	}
	for _, b := range bodies {
		if b != `{"n":1}` {
			t.Fatalf("body not replayed identically: %q", b)
		}
	}
	if !out["ok"] {
		t.Fatalf("expected decoded ok=true")
	}
}

func TestRetry_NonReplayableBodyNotRetried(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := newClient(t, srv.URL, Deps{}, WithRetry(fastRetry()))
	// PUT (idempotent) but body is a bare io.Reader => no GetBody => single attempt.
	body := struct{ io.Reader }{strings.NewReader("payload")}
	resp, err := c.Do(context.Background(), &Request{Method: http.MethodPut, Path: "/x", Body: body})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if hits, _, _ := rec.snapshot(); hits != 1 {
		t.Fatalf("expected 1 attempt for non-replayable body, got %d", hits)
	}
}

func TestRetry_RespectsContextDeadline(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rcfg := fastRetry()
	rcfg.BaseBackoff = 500 * time.Millisecond
	rcfg.MaxBackoff = time.Second // ensure the backoff (500ms) exceeds the ctx deadline
	rcfg.Jitter = false
	c := newClient(t, srv.URL, Deps{}, WithRetry(rcfg))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := c.Do(ctx, &Request{Method: http.MethodGet, Path: "/x"})
	if err == nil {
		t.Fatalf("expected deadline error")
	}
	if hits, _, _ := rec.snapshot(); hits != 1 {
		t.Fatalf("expected 1 attempt before deadline, got %d", hits)
	}
}

func TestJSON_DecodeAndHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"name":"pulse"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"missing"}`))
	}))
	defer srv.Close()
	c := newClient(t, srv.URL, Deps{})

	var out struct{ Name string }
	if err := c.GetJSON(context.Background(), "/ok", &out); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if out.Name != "pulse" {
		t.Fatalf("decode failed: %+v", out)
	}

	err := c.GetJSON(context.Background(), "/missing", &out)
	var he *HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("expected *HTTPError, got %v", err)
	}
	if he.StatusCode != http.StatusNotFound || !strings.Contains(he.Snippet, "missing") {
		t.Fatalf("unexpected HTTPError: %+v", he)
	}
}

func TestMetrics_RecordedByHostMethodStatus(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if rec.hits < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	m, err := NewMetrics(DefaultConfig().Metrics, nil)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	c := newClient(t, srv.URL, Deps{Metrics: m}, WithRetry(fastRetry()))

	var out map[string]any
	if err := c.PutJSON(context.Background(), "/x", map[string]int{}, &out); err != nil {
		t.Fatalf("PutJSON: %v", err)
	}

	host := mustHost(t, srv.URL)
	if got := testutil.ToFloat64(m.total.WithLabelValues(host, "PUT", "200")); got != 1 {
		t.Fatalf("expected 1 logical request counted, got %v", got)
	}
	if strings.Contains(host, "/") {
		t.Fatalf("host label must not contain path: %q", host)
	}
	if got := testutil.ToFloat64(m.retries.WithLabelValues(host, "PUT")); got != 2 {
		t.Fatalf("expected 2 retries counted, got %v", got)
	}
}

func TestPoolFieldsApplied(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithPoolSize(33, 7), WithBaseURL("http://x"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	bt := baseTransport(t, c)
	if bt.MaxIdleConnsPerHost != 33 || bt.MaxConnsPerHost != 7 {
		t.Fatalf("pool not applied: idlePerHost=%d perHost=%d", bt.MaxIdleConnsPerHost, bt.MaxConnsPerHost)
	}
	if bt.IdleConnTimeout != DefaultConfig().Pool.IdleConnTimeout {
		t.Fatalf("idle conn timeout default not applied")
	}
}

func TestDefaultsAndOptions(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Trace.TraceIDHeader != "X-Trace-Id" || !boolValue(cfg.Retry.Enabled) {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	c, _ := New(Config{}, Deps{}, WithTraceIDHeader("X-My-Trace"))
	if c.Config().Trace.TraceIDHeader != "X-My-Trace" {
		t.Fatalf("option not applied")
	}
	// zero nested fields get defaulted
	if c.Config().Timeouts.Request != DefaultConfig().Timeouts.Request {
		t.Fatalf("nested default not applied")
	}
	// Tri-state bool defaults are restored from a zero-value Config too.
	if !boolValue(c.Config().Trace.InjectTraceparent) || !boolValue(c.Config().Trace.Propagate) ||
		!boolValue(c.Config().Retry.Enabled) || !boolValue(c.Config().Retry.RespectRetryAfter) {
		t.Fatalf("tri-state bool defaults not applied: %+v", c.Config())
	}
}

func TestResolveURL_KeepsBasePathPrefix(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithBaseURL("http://api.example.com/v1"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Both a rooted ("/users") and a relative ("users") path must append onto the
	// base path prefix, not replace it (the ResolveReference footgun).
	for _, path := range []string{"/users", "users"} {
		got, err := c.resolveURL(path, nil)
		if err != nil {
			t.Fatalf("resolveURL(%q): %v", path, err)
		}
		if want := "http://api.example.com/v1/users"; got != want {
			t.Fatalf("resolveURL(%q) = %q, want %q", path, got, want)
		}
	}
}

// --- helpers ---------------------------------------------------------------

func fastRetry() RetryConfig {
	r := DefaultConfig().Retry
	r.BaseBackoff = time.Millisecond
	r.MaxBackoff = 5 * time.Millisecond
	r.Jitter = false
	return r
}

func baseTransport(t *testing.T, c *Client) *http.Transport {
	t.Helper()
	rt := c.httpc.Transport
	for i := 0; i < 8; i++ {
		switch v := rt.(type) {
		case *obsRT:
			rt = v.next
		case *retryRT:
			rt = v.next
		case *manualTraceRT:
			rt = v.next
		case *headerRT:
			rt = v.next
		case *http.Transport:
			return v
		default:
			t.Fatalf("unexpected roundtripper %T", v)
		}
	}
	t.Fatalf("base transport not found")
	return nil
}

func mustHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return u.Hostname()
}
