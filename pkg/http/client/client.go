package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/tuanlao/pulse/pkg/log"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Deps are optional collaborators. Nil collaborators degrade gracefully.
type Deps struct {
	// Logger is the base logger for outbound request logs; nil disables logging.
	Logger *log.Logger
	// Metrics enables outbound RED metrics; nil disables them.
	Metrics *ClientMetrics
	// TracerProvider, when a real (exporting) provider, makes the client create
	// client spans via otelhttp. nil or a no-op provider uses manual id injection.
	TracerProvider trace.TracerProvider
	// Propagator injects the W3C trace context. Default propagation.TraceContext{}.
	Propagator propagation.TextMapPropagator
}

// Client is a configurable outbound HTTP client.
type Client struct {
	cfg     Config
	deps    Deps
	base    *url.URL
	httpc   *http.Client
	tracing bool
}

// New builds a Client. It parses BaseURL, assembles the RoundTripper stack
// (obs → retry → trace → pooled transport) and detects whether a real tracer
// provider was supplied.
func New(cfg Config, deps Deps, opts ...Option) (*Client, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if deps.Propagator == nil {
		deps.Propagator = propagation.TraceContext{}
	}

	var base *url.URL
	if cfg.BaseURL != "" {
		u, err := url.Parse(cfg.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("client: invalid base_url %q: %w", cfg.BaseURL, err)
		}
		base = u
	}

	c := &Client{
		cfg:     cfg,
		deps:    deps,
		base:    base,
		tracing: isRealProvider(deps.TracerProvider),
	}
	c.httpc = &http.Client{Transport: c.buildTransport()}
	return c, nil
}

// buildTransport assembles the RoundTripper stack from innermost (pooled
// transport) to outermost (observability).
func (c *Client) buildTransport() http.RoundTripper {
	var rt http.RoundTripper = newBaseTransport(c.cfg)

	if c.tracing {
		// otelhttp owns the client span + traceparent; headerRT adds X-Trace-Id.
		rt = otelhttp.NewTransport(rt,
			otelhttp.WithTracerProvider(c.deps.TracerProvider),
			otelhttp.WithPropagators(c.deps.Propagator),
		)
		rt = &headerRT{next: rt, cfg: c.cfg.Trace}
	} else {
		rt = &manualTraceRT{next: rt, cfg: c.cfg.Trace}
	}

	rt = &retryRT{next: rt, cfg: c.cfg.Retry, metrics: c.deps.Metrics}
	rt = &obsRT{next: rt, metrics: c.deps.Metrics, logger: c.deps.Logger}
	return rt
}

func newBaseTransport(cfg Config) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.Timeouts.Dial,
			KeepAlive: cfg.Timeouts.KeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          cfg.Pool.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.Pool.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.Pool.MaxConnsPerHost,
		IdleConnTimeout:       cfg.Pool.IdleConnTimeout,
		DisableKeepAlives:     cfg.Pool.DisableKeepAlives,
		TLSHandshakeTimeout:   cfg.Timeouts.TLSHandshake,
		ResponseHeaderTimeout: cfg.Timeouts.ResponseHeader,
		ExpectContinueTimeout: cfg.Timeouts.ExpectContinue,
	}
}

// isRealProvider reports whether tp is a real (exporting) provider rather than
// the OTel no-op. It detects the no-op by type so it does NOT create a throwaway
// "probe" span — which would otherwise be exported on every client construction.
// pulse's tracing package returns noop.NewTracerProvider() when disabled, which
// is matched here.
func isRealProvider(tp trace.TracerProvider) bool {
	if tp == nil {
		return false
	}
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return false
	}
	return true
}

// HTTPClient returns the underlying *http.Client (escape hatch).
func (c *Client) HTTPClient() *http.Client { return c.httpc }

// Config returns the resolved configuration.
func (c *Client) Config() Config { return c.cfg }

// Request describes an outbound call.
type Request struct {
	Method string
	Path   string
	// Body is an arbitrary request body. For retry-safe replay prefer JSONBody or
	// a *bytes.Reader/*strings.Reader (which set GetBody automatically).
	Body io.Reader
	// JSONBody, when non-nil, is JSON-marshaled into the body (retry-safe).
	JSONBody any
	Header   http.Header
	Query    url.Values

	noRetry bool
}

// RequestOption mutates a Request.
type RequestOption func(*Request)

// WithHeader adds a request header.
func WithHeader(key, value string) RequestOption {
	return func(r *Request) {
		if r.Header == nil {
			r.Header = http.Header{}
		}
		r.Header.Add(key, value)
	}
}

// WithQuery adds a query parameter.
func WithQuery(key, value string) RequestOption {
	return func(r *Request) {
		if r.Query == nil {
			r.Query = url.Values{}
		}
		r.Query.Add(key, value)
	}
}

// WithNoRetry disables retries for this call.
func WithNoRetry() RequestOption { return func(r *Request) { r.noRetry = true } }

// Do executes a Request. A nil ctx becomes context.Background(); the per-call
// timeout, trace id, URL resolution and default headers are all applied here.
// The caller owns closing the returned response body.
func (c *Client) Do(ctx context.Context, r *Request) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.cfg.Timeouts.Request > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.cfg.Timeouts.Request)
		defer cancel()
	}
	ctx = ensureIDs(ctx)
	if r.noRetry {
		ctx = withNoRetry(ctx)
	}

	u, err := c.resolveURL(r.Path, r.Query)
	if err != nil {
		return nil, err
	}

	body, err := r.bodyReader()
	if err != nil {
		return nil, err
	}

	method := r.Method
	if method == "" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	c.applyHeaders(req, r)

	return c.httpc.Do(req)
}

// resolveURL joins path onto BaseURL (when path is relative) and appends query.
func (c *Client) resolveURL(path string, query url.Values) (string, error) {
	ref, err := url.Parse(path)
	if err != nil {
		return "", fmt.Errorf("client: invalid path %q: %w", path, err)
	}
	var full *url.URL
	if ref.IsAbs() || c.base == nil {
		full = ref
	} else {
		// JoinPath appends the path onto BaseURL's path instead of replacing it.
		// ResolveReference would drop a base path prefix for a rooted ("/x") or
		// last-segment reference (RFC 3986), so base ".../v1" + "/users" (or
		// "users") would become ".../users". JoinPath keeps it: ".../v1/users".
		full = c.base.JoinPath(ref.EscapedPath())
		full.RawQuery = ref.RawQuery
		full.Fragment = ref.Fragment
	}
	if len(query) > 0 {
		q := full.Query()
		for k, vs := range query {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
		full.RawQuery = q.Encode()
	}
	return full.String(), nil
}

func (r *Request) bodyReader() (io.Reader, error) {
	if r.JSONBody != nil {
		b, err := json.Marshal(r.JSONBody)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(b), nil // *bytes.Reader => GetBody set automatically
	}
	return r.Body, nil
}

func (c *Client) applyHeaders(req *http.Request, r *Request) {
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if r.JSONBody != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("User-Agent") == "" && c.cfg.UserAgent != "" {
		req.Header.Set("User-Agent", c.cfg.UserAgent)
	}
}

// --- JSON helpers -----------------------------------------------------------

// GetJSON issues a GET and decodes a 2xx JSON body into out (out may be nil).
func (c *Client) GetJSON(ctx context.Context, path string, out any, opts ...RequestOption) error {
	return c.doJSON(ctx, http.MethodGet, path, nil, out, opts...)
}

// PostJSON issues a POST with in marshaled as JSON and decodes the response.
func (c *Client) PostJSON(ctx context.Context, path string, in, out any, opts ...RequestOption) error {
	return c.doJSON(ctx, http.MethodPost, path, in, out, opts...)
}

// PutJSON issues a PUT with in marshaled as JSON and decodes the response.
func (c *Client) PutJSON(ctx context.Context, path string, in, out any, opts ...RequestOption) error {
	return c.doJSON(ctx, http.MethodPut, path, in, out, opts...)
}

// DeleteJSON issues a DELETE and decodes a 2xx JSON body into out.
func (c *Client) DeleteJSON(ctx context.Context, path string, out any, opts ...RequestOption) error {
	return c.doJSON(ctx, http.MethodDelete, path, nil, out, opts...)
}

const errSnippetLen = 512

func (c *Client) doJSON(ctx context.Context, method, path string, in, out any, opts ...RequestOption) error {
	req := &Request{Method: method, Path: path, JSONBody: in, Header: http.Header{}}
	req.Header.Set("Accept", "application/json")
	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.Do(ctx, req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, errSnippetLen))
		return &HTTPError{
			Method:     method,
			URL:        resp.Request.URL.String(),
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Snippet:    strings.TrimSpace(string(snippet)),
		}
	}

	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("client: decode response: %w", err)
	}
	return nil
}

// --- per-request no-retry flag in context -----------------------------------

type ctxKey int

const noRetryKey ctxKey = iota

func withNoRetry(ctx context.Context) context.Context {
	return context.WithValue(ctx, noRetryKey, true)
}

func reqNoRetry(ctx context.Context) bool {
	v, _ := ctx.Value(noRetryKey).(bool)
	return v
}
