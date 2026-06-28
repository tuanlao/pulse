// Package client is a configurable outbound gRPC client for calling other
// services. It owns a *grpc.ClientConn (built with the modern, lazy
// grpc.NewClient), wires otelgrpc tracing via a StatsHandler (or manual
// x-trace-id correlation when no real tracer is present), applies a per-call
// request timeout, retry via the gRPC service config, keepalive, message-size
// caps and TLS, and exposes client-side RED metrics plus outbound logging. It
// implements lifecycle.Component so the manager closes the connection on
// shutdown.
//
// Like every pulse package it exposes Config + DefaultConfig() + functional
// Options, and Config is nested-object shaped so it maps onto structured YAML.
package client

import (
	"encoding/json"
	"fmt"
	"time"
)

// TLSConfig configures client-side TLS. When Enabled is false the client dials
// with insecure credentials (suitable for local dev / in-mesh traffic). With
// Enabled and an empty CAFile the system root pool is used (CAFile is optional).
type TLSConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	CAFile             string `mapstructure:"ca_file"`
	ServerNameOverride string `mapstructure:"server_name_override"`
	InsecureSkipVerify bool   `mapstructure:"insecure_skip_verify"`
	// CertFile/KeyFile enable a client certificate (mTLS). Seam.
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}

// TimeoutsConfig holds the per-call timeout.
type TimeoutsConfig struct {
	// Request bounds a single call via context, applied only when the caller's
	// context has no deadline. Default 30s. Streaming RPCs are NOT auto-bounded.
	Request time.Duration `mapstructure:"request"`
}

// KeepaliveConfig maps onto google.golang.org/grpc/keepalive ClientParameters.
type KeepaliveConfig struct {
	// Time pings the server after this much idle time. Default 30s.
	Time time.Duration `mapstructure:"time"`
	// Timeout waits this long for a ping ack before considering the conn dead. Default 20s.
	Timeout time.Duration `mapstructure:"timeout"`
	// PermitWithoutStream allows pings with no active RPCs. Default true.
	PermitWithoutStream bool `mapstructure:"permit_without_stream"`
}

// RetryConfig controls the gRPC service-config retry policy.
type RetryConfig struct {
	// Enabled toggles retries. Default true (nil means "unset", backfilled).
	Enabled *bool `mapstructure:"enabled"`
	// MaxAttempts is the total attempts including the first. Default 3 (>= 2 to be
	// meaningful; gRPC clamps the channel maximum).
	MaxAttempts int `mapstructure:"max_attempts"`
	// InitialBackoff is the first retry backoff. Default 100ms.
	InitialBackoff time.Duration `mapstructure:"initial_backoff"`
	// MaxBackoff caps the exponential backoff. Default 2s.
	MaxBackoff time.Duration `mapstructure:"max_backoff"`
	// BackoffMultiplier grows the backoff each attempt. Default 2.0.
	BackoffMultiplier float64 `mapstructure:"backoff_multiplier"`
	// RetryableStatusCodes are the gRPC codes that trigger a retry. Default ["UNAVAILABLE"].
	RetryableStatusCodes []string `mapstructure:"retryable_status_codes"`
	// RawServiceConfig overrides the generated policy with a raw gRPC service-config
	// JSON, used verbatim. Seam for hedging / throttling / per-method config /
	// waitForReady. Empty means "use the generated policy".
	RawServiceConfig string `mapstructure:"raw_service_config"`
}

// TraceConfig controls trace-id propagation on outgoing calls.
type TraceConfig struct {
	// Propagate emits the custom trace-id metadata header on the tracing-OFF path.
	// Default true (nil means "unset", backfilled). On the tracing-ON path otelgrpc
	// emits the W3C traceparent and the custom header is not added.
	Propagate *bool `mapstructure:"propagate"`
	// TraceIDHeader is the custom trace-id metadata key (gRPC lowercases keys).
	// Default "x-trace-id".
	TraceIDHeader string `mapstructure:"trace_id_header"`
}

// MetricsConfig controls client-side RED metrics.
type MetricsConfig struct {
	// Enabled toggles outbound metrics. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "grpc_client".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// Config configures the client. All durations have defaults; sub-objects keep
// their own defaulting local.
type Config struct {
	// Target is the dial target, e.g. "dns:///svc:9090" or "127.0.0.1:9090".
	Target string `mapstructure:"target"`
	// UserAgent is sent on every call. Default "pulse-grpc-client".
	UserAgent string `mapstructure:"user_agent"`

	// MaxRecvMsgSize caps a received message in bytes. Default 4 MiB.
	MaxRecvMsgSize int `mapstructure:"max_recv_msg_size"`
	// MaxSendMsgSize caps a sent message in bytes. Default 4 MiB.
	MaxSendMsgSize int `mapstructure:"max_send_msg_size"`

	Timeouts  TimeoutsConfig  `mapstructure:"timeouts"`
	Keepalive KeepaliveConfig `mapstructure:"keepalive"`
	Retry     RetryConfig     `mapstructure:"retry"`
	Trace     TraceConfig     `mapstructure:"trace"`
	Metrics   MetricsConfig   `mapstructure:"metrics"`
	TLS       TLSConfig       `mapstructure:"tls"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		UserAgent:      "pulse-grpc-client",
		MaxRecvMsgSize: 4 << 20,
		MaxSendMsgSize: 4 << 20,
		Timeouts:       TimeoutsConfig{Request: 30 * time.Second},
		Keepalive: KeepaliveConfig{
			Time:                30 * time.Second,
			Timeout:             20 * time.Second,
			PermitWithoutStream: true,
		},
		Retry: RetryConfig{
			Enabled:              boolPtr(true),
			MaxAttempts:          3,
			InitialBackoff:       100 * time.Millisecond,
			MaxBackoff:           2 * time.Second,
			BackoffMultiplier:    2.0,
			RetryableStatusCodes: []string{"UNAVAILABLE"},
		},
		Trace: TraceConfig{
			Propagate:     boolPtr(true),
			TraceIDHeader: "x-trace-id",
		},
		Metrics: MetricsConfig{
			Enabled:   true,
			Namespace: "pulse",
			Subsystem: "grpc_client",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		TLS: TLSConfig{},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.UserAgent == "" {
		c.UserAgent = d.UserAgent
	}
	if c.MaxRecvMsgSize == 0 {
		c.MaxRecvMsgSize = d.MaxRecvMsgSize
	}
	if c.MaxSendMsgSize == 0 {
		c.MaxSendMsgSize = d.MaxSendMsgSize
	}
	c.Timeouts.applyDefaults(d.Timeouts)
	c.Keepalive.applyDefaults(d.Keepalive)
	c.Retry.applyDefaults(d.Retry)
	c.Trace.applyDefaults(d.Trace)
	c.Metrics.applyDefaults(d.Metrics)
}

func (t *TimeoutsConfig) applyDefaults(d TimeoutsConfig) {
	if t.Request <= 0 {
		t.Request = d.Request
	}
}

func (k *KeepaliveConfig) applyDefaults(d KeepaliveConfig) {
	if k.Time <= 0 {
		k.Time = d.Time
	}
	if k.Timeout <= 0 {
		k.Timeout = d.Timeout
	}
	// PermitWithoutStream defaults to true; not re-applied (see DefaultConfig caveat).
}

func (r *RetryConfig) applyDefaults(d RetryConfig) {
	if r.MaxAttempts <= 0 {
		r.MaxAttempts = d.MaxAttempts
	}
	if r.InitialBackoff <= 0 {
		r.InitialBackoff = d.InitialBackoff
	}
	if r.MaxBackoff <= 0 {
		r.MaxBackoff = d.MaxBackoff
	}
	if r.BackoffMultiplier <= 0 {
		r.BackoffMultiplier = d.BackoffMultiplier
	}
	if len(r.RetryableStatusCodes) == 0 {
		r.RetryableStatusCodes = d.RetryableStatusCodes
	}
	if r.Enabled == nil {
		r.Enabled = d.Enabled
	}
}

func (t *TraceConfig) applyDefaults(d TraceConfig) {
	if t.TraceIDHeader == "" {
		t.TraceIDHeader = d.TraceIDHeader
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

// boolValue dereferences a tri-state bool, treating nil as false.
func boolValue(p *bool) bool { return p != nil && *p }

// Option overrides Config fields.
type Option func(*Config)

// WithTarget sets the dial target.
func WithTarget(t string) Option { return func(c *Config) { c.Target = t } }

// WithRequestTimeout sets the per-call timeout.
func WithRequestTimeout(d time.Duration) Option {
	return func(c *Config) { c.Timeouts.Request = d }
}

// WithRetry overrides the retry config.
func WithRetry(r RetryConfig) Option { return func(c *Config) { c.Retry = r } }

// WithServiceConfig sets a raw gRPC service-config JSON, overriding the generated
// retry policy verbatim.
func WithServiceConfig(rawJSON string) Option {
	return func(c *Config) { c.Retry.RawServiceConfig = rawJSON }
}

// WithTLS overrides the TLS config.
func WithTLS(t TLSConfig) Option { return func(c *Config) { c.TLS = t } }

// WithTraceIDHeader sets the custom trace-id metadata key.
func WithTraceIDHeader(name string) Option {
	return func(c *Config) { c.Trace.TraceIDHeader = name }
}

// WithUserAgent sets the user agent.
func WithUserAgent(ua string) Option { return func(c *Config) { c.UserAgent = ua } }

// serviceConfigJSON builds the gRPC service-config JSON enabling the retry policy
// for all methods. It returns "" when retry is disabled. A non-empty
// RawServiceConfig overrides the generated policy verbatim.
func (c *Config) serviceConfigJSON() string {
	if c.Retry.RawServiceConfig != "" {
		return c.Retry.RawServiceConfig
	}
	if !boolValue(c.Retry.Enabled) {
		return ""
	}

	// gRPC's service-config schema uses camelCase JSON keys and "Ns" duration
	// strings. The empty name object ({}) applies the policy to all methods.
	type retryPolicy struct {
		MaxAttempts          int      `json:"maxAttempts"`
		InitialBackoff       string   `json:"initialBackoff"`
		MaxBackoff           string   `json:"maxBackoff"`
		BackoffMultiplier    float64  `json:"backoffMultiplier"`
		RetryableStatusCodes []string `json:"retryableStatusCodes"`
	}
	type methodConfig struct {
		Name        []map[string]any `json:"name"`
		RetryPolicy retryPolicy      `json:"retryPolicy"`
	}
	type serviceConfig struct {
		MethodConfig []methodConfig `json:"methodConfig"`
	}

	sc := serviceConfig{MethodConfig: []methodConfig{{
		Name: []map[string]any{{}},
		RetryPolicy: retryPolicy{
			MaxAttempts:          c.Retry.MaxAttempts,
			InitialBackoff:       durationSeconds(c.Retry.InitialBackoff),
			MaxBackoff:           durationSeconds(c.Retry.MaxBackoff),
			BackoffMultiplier:    c.Retry.BackoffMultiplier,
			RetryableStatusCodes: c.Retry.RetryableStatusCodes,
		},
	}}}
	b, err := json.Marshal(sc)
	if err != nil {
		return ""
	}
	return string(b)
}

// durationSeconds renders d as a gRPC service-config duration string (e.g. "0.1s").
func durationSeconds(d time.Duration) string {
	return fmt.Sprintf("%gs", d.Seconds())
}
