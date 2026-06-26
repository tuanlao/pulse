// Package client is a configurable outbound HTTP client for calling other
// services. It provides connection pooling, per-call context handling (creating
// one if absent), guaranteed trace-id propagation (W3C traceparent + a
// customizable X-Trace-Id header, generating ids when the context has none),
// retries with backoff, JSON helpers, client-side RED metrics and outbound
// request logging.
//
// Like every pulse package it exposes Config + DefaultConfig() + functional
// Options, and Config is nested-object shaped (pool/retry/trace/... sub-objects)
// so it maps onto structured YAML rather than flat keys.
package client

import (
	"net/http"
	"time"
)

// Config configures the client. All durations have defaults; sub-objects keep
// their own defaulting local.
type Config struct {
	// BaseURL is the domain/base URL; relative request paths resolve against it.
	BaseURL string `mapstructure:"base_url"`
	// UserAgent is sent on every request. Default "pulse-client".
	UserAgent string `mapstructure:"user_agent"`

	Timeouts TimeoutsConfig `mapstructure:"timeouts"`
	Pool     PoolConfig     `mapstructure:"pool"`
	Retry    RetryConfig    `mapstructure:"retry"`
	Trace    TraceConfig    `mapstructure:"trace"`
	Metrics  MetricsConfig  `mapstructure:"metrics"`
}

// TimeoutsConfig holds the per-call and transport timeouts.
type TimeoutsConfig struct {
	// Request bounds the whole call (all retries) via context. Default 30s.
	Request time.Duration `mapstructure:"request"`
	// Dial is the TCP dial timeout. Default 5s.
	Dial time.Duration `mapstructure:"dial"`
	// TLSHandshake is the TLS handshake timeout. Default 5s.
	TLSHandshake time.Duration `mapstructure:"tls_handshake"`
	// ResponseHeader bounds waiting for response headers per attempt. Default 10s.
	ResponseHeader time.Duration `mapstructure:"response_header"`
	// ExpectContinue is the 100-continue timeout. Default 1s.
	ExpectContinue time.Duration `mapstructure:"expect_continue"`
	// KeepAlive is the dialer keep-alive period. Default 30s.
	KeepAlive time.Duration `mapstructure:"keep_alive"`
}

// PoolConfig holds connection-pool sizing.
type PoolConfig struct {
	// MaxIdleConns is the total idle-conn cap. Default 100.
	MaxIdleConns int `mapstructure:"max_idle_conns"`
	// MaxIdleConnsPerHost caps idle conns per host. Default 10 (stdlib default of
	// 2 is too low for a service client).
	MaxIdleConnsPerHost int `mapstructure:"max_idle_conns_per_host"`
	// MaxConnsPerHost caps total conns per host. Default 0 (unlimited).
	MaxConnsPerHost int `mapstructure:"max_conns_per_host"`
	// IdleConnTimeout is how long an idle conn is kept. Default 90s.
	IdleConnTimeout time.Duration `mapstructure:"idle_conn_timeout"`
	// DisableKeepAlives disables connection reuse. Default false.
	DisableKeepAlives bool `mapstructure:"disable_keep_alives"`
}

// RetryConfig controls automatic retries.
type RetryConfig struct {
	// Enabled toggles retries. Default true. A nil pointer means "unset" and is
	// backfilled from the default by applyDefaults, so the default holds even when
	// the config is unmarshaled from a partial source (e.g. a HttpClients map
	// entry) that omits the key.
	Enabled *bool `mapstructure:"enabled"`
	// MaxAttempts is the total attempts including the first. Default 3.
	MaxAttempts int `mapstructure:"max_attempts"`
	// BaseBackoff is the initial backoff. Default 100ms.
	BaseBackoff time.Duration `mapstructure:"base_backoff"`
	// MaxBackoff caps the exponential backoff. Default 2s.
	MaxBackoff time.Duration `mapstructure:"max_backoff"`
	// Jitter applies full jitter to backoff. Default true.
	Jitter bool `mapstructure:"jitter"`
	// Methods are the retryable (idempotent) methods. Default
	// GET,HEAD,PUT,DELETE,OPTIONS.
	Methods []string `mapstructure:"methods"`
	// RetryStatuses are the retryable status codes. Default 502,503,504.
	RetryStatuses []int `mapstructure:"retry_statuses"`
	// RespectRetryAfter honors the Retry-After response header. Default true (nil
	// means "unset", backfilled to the default by applyDefaults).
	RespectRetryAfter *bool `mapstructure:"respect_retry_after"`
}

// TraceConfig controls trace-id propagation on outgoing requests.
type TraceConfig struct {
	// InjectTraceparent emits the W3C traceparent header. Default true (nil means
	// "unset", backfilled to the default by applyDefaults).
	InjectTraceparent *bool `mapstructure:"inject_traceparent"`
	// Propagate emits the custom trace-id header. Default true (nil means "unset",
	// backfilled to the default by applyDefaults).
	Propagate *bool `mapstructure:"propagate"`
	// TraceIDHeader is the custom trace-id header name. Default "X-Trace-Id".
	TraceIDHeader string `mapstructure:"trace_id_header"`
}

// MetricsConfig controls client-side RED metrics.
type MetricsConfig struct {
	// Enabled toggles outbound metrics. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "http_client".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		UserAgent: "pulse-client",
		Timeouts: TimeoutsConfig{
			Request:        30 * time.Second,
			Dial:           5 * time.Second,
			TLSHandshake:   5 * time.Second,
			ResponseHeader: 10 * time.Second,
			ExpectContinue: 1 * time.Second,
			KeepAlive:      30 * time.Second,
		},
		Pool: PoolConfig{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			MaxConnsPerHost:     0,
			IdleConnTimeout:     90 * time.Second,
		},
		Retry: RetryConfig{
			Enabled:           boolPtr(true),
			MaxAttempts:       3,
			BaseBackoff:       100 * time.Millisecond,
			MaxBackoff:        2 * time.Second,
			Jitter:            true,
			Methods:           []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions},
			RetryStatuses:     []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
			RespectRetryAfter: boolPtr(true),
		},
		Trace: TraceConfig{
			InjectTraceparent: boolPtr(true),
			Propagate:         boolPtr(true),
			TraceIDHeader:     "X-Trace-Id",
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "pulse",
			Subsystem: "http_client",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.UserAgent == "" {
		c.UserAgent = d.UserAgent
	}
	c.Timeouts.applyDefaults(d.Timeouts)
	c.Pool.applyDefaults(d.Pool)
	c.Retry.applyDefaults(d.Retry)
	c.Trace.applyDefaults(d.Trace)
	c.Metrics.applyDefaults(d.Metrics)
}

func (t *TimeoutsConfig) applyDefaults(d TimeoutsConfig) {
	if t.Request <= 0 {
		t.Request = d.Request
	}
	if t.Dial <= 0 {
		t.Dial = d.Dial
	}
	if t.TLSHandshake <= 0 {
		t.TLSHandshake = d.TLSHandshake
	}
	if t.ResponseHeader <= 0 {
		t.ResponseHeader = d.ResponseHeader
	}
	if t.ExpectContinue <= 0 {
		t.ExpectContinue = d.ExpectContinue
	}
	if t.KeepAlive <= 0 {
		t.KeepAlive = d.KeepAlive
	}
}

func (p *PoolConfig) applyDefaults(d PoolConfig) {
	if p.MaxIdleConns == 0 {
		p.MaxIdleConns = d.MaxIdleConns
	}
	if p.MaxIdleConnsPerHost == 0 {
		p.MaxIdleConnsPerHost = d.MaxIdleConnsPerHost
	}
	if p.IdleConnTimeout <= 0 {
		p.IdleConnTimeout = d.IdleConnTimeout
	}
}

func (r *RetryConfig) applyDefaults(d RetryConfig) {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = d.MaxAttempts
	}
	if r.BaseBackoff <= 0 {
		r.BaseBackoff = d.BaseBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = d.MaxBackoff
	}
	if len(r.Methods) == 0 {
		r.Methods = d.Methods
	}
	if len(r.RetryStatuses) == 0 {
		r.RetryStatuses = d.RetryStatuses
	}
	if r.Enabled == nil {
		r.Enabled = d.Enabled
	}
	if r.RespectRetryAfter == nil {
		r.RespectRetryAfter = d.RespectRetryAfter
	}
}

func (t *TraceConfig) applyDefaults(d TraceConfig) {
	if t.TraceIDHeader == "" {
		t.TraceIDHeader = d.TraceIDHeader
	}
	if t.InjectTraceparent == nil {
		t.InjectTraceparent = d.InjectTraceparent
	}
	if t.Propagate == nil {
		t.Propagate = d.Propagate
	}
}

func (m *MetricsConfig) applyDefaults(d MetricsConfig) {
	if m.Namespace == "" {
		m.Namespace = d.Namespace
	}
	if m.Subsystem == "" {
		m.Subsystem = d.Subsystem
	}
	if len(m.Buckets) == 0 {
		m.Buckets = d.Buckets
	}
}

// boolPtr returns a pointer to b. Used by DefaultConfig for tri-state bool fields
// whose zero value (false) differs from their default (true).
func boolPtr(b bool) *bool { return &b }

// boolValue dereferences a tri-state bool, treating nil as false. After
// applyDefaults the pointers are always non-nil; the nil guard is defensive.
func boolValue(p *bool) bool { return p != nil && *p }

// Option overrides Config fields.
type Option func(*Config)

// WithBaseURL sets the base URL.
func WithBaseURL(u string) Option { return func(c *Config) { c.BaseURL = u } }

// WithPoolSize sets max idle/total conns per host.
func WithPoolSize(maxIdlePerHost, maxPerHost int) Option {
	return func(c *Config) {
		c.Pool.MaxIdleConnsPerHost = maxIdlePerHost
		c.Pool.MaxConnsPerHost = maxPerHost
	}
}

// WithRequestTimeout sets the per-call timeout.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Config) { c.Timeouts.Request = d }
}

// WithRetry overrides the retry config.
func WithRetry(r RetryConfig) Option { return func(c *Config) { c.Retry = r } }

// WithTraceIDHeader sets the custom trace-id header name.
func WithTraceIDHeader(name string) Option {
	return func(c *Config) { c.Trace.TraceIDHeader = name }
}
