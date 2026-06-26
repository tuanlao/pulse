// Package tracing configures an OpenTelemetry TracerProvider that exports spans
// over OTLP (gRPC or HTTP). It is enabled by default and integrates with the
// lifecycle manager: Stop flushes and shuts the provider down (TracerProvider
// .Shutdown), satisfying the "flush traces on shutdown" requirement.
//
// Three states:
//   - Enabled with a non-empty Endpoint: a real SDK provider exports spans to
//     the OTLP collector.
//   - Enabled with an empty Endpoint: a real SDK provider still generates spans
//     (so trace_id/span_id flow into logs) but nothing is exported — no
//     collector required.
//   - Disabled: New returns a Tracer backed by a no-op provider (no spans, no
//     trace id) so callers can always wire it unconditionally.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Protocol selects the OTLP transport.
type Protocol string

const (
	// ProtocolGRPC exports via OTLP/gRPC (default).
	ProtocolGRPC Protocol = "grpc"
	// ProtocolHTTP exports via OTLP/HTTP (protobuf).
	ProtocolHTTP Protocol = "http"
)

// Config configures tracing.
type Config struct {
	// Enabled toggles tracing. Default true.
	Enabled bool `mapstructure:"enabled"`
	// ServiceName is the OTel service.name resource attribute. REQUIRED when
	// enabled.
	ServiceName string `mapstructure:"service_name"`
	// ServiceVersion is the service.version resource attribute.
	ServiceVersion string `mapstructure:"service_version"`
	// Environment is the deployment.environment resource attribute.
	Environment string `mapstructure:"environment"`
	// Protocol is "grpc" or "http". Default "grpc".
	Protocol Protocol `mapstructure:"protocol"`
	// Endpoint is the OTLP collector endpoint, e.g. "localhost:4317" (grpc) or
	// "localhost:4318" (http). Default "" (empty), which keeps tracing on (spans
	// are still generated, so trace_id/span_id reach the logs) but disables OTLP
	// export — no collector is contacted. Set a non-empty endpoint to opt into
	// exporting spans to a collector.
	Endpoint string `mapstructure:"endpoint"`
	// Insecure disables transport security (plaintext). Default true (dev).
	Insecure bool `mapstructure:"insecure"`
	// Headers are extra OTLP exporter headers (e.g. auth tokens for a vendor).
	Headers map[string]string `mapstructure:"headers"`
	// SampleRatio is the parent-based ratio sampler in [0,1]. Default 1.0. A ratio
	// of 0 is honored (never sample); a negative value means "unset" and is
	// backfilled from the default. Build from DefaultConfig() (the source of truth)
	// to get 1.0 rather than a zero-value Config{}.
	SampleRatio float64 `mapstructure:"sample_ratio"`
	// ExportTimeout bounds a single export. Default 30s.
	ExportTimeout time.Duration `mapstructure:"export_timeout"`
}

// DefaultConfig returns Config with sensible defaults (tracing enabled).
func DefaultConfig() Config {
	return Config{
		Enabled:       true,
		Protocol:      ProtocolGRPC,
		Endpoint:      "",
		Insecure:      true,
		SampleRatio:   1.0,
		ExportTimeout: 30 * time.Second,
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Protocol == "" {
		c.Protocol = d.Protocol
	}
	// Endpoint is intentionally NOT backfilled: an empty endpoint is a valid
	// state meaning "tracing on, OTLP export off" — and is in fact the default.
	// SampleRatio uses a negative sentinel for "unset": 0 is a meaningful value
	// (never sample) and must be honored, so only a negative ratio is backfilled
	// to the default. Out-of-range values are clamped to [0,1].
	if c.SampleRatio < 0 {
		c.SampleRatio = d.SampleRatio
	}
	if c.SampleRatio > 1 {
		c.SampleRatio = 1
	}
	if c.ExportTimeout <= 0 {
		c.ExportTimeout = d.ExportTimeout
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithEnabled toggles tracing.
func WithEnabled(enabled bool) Option { return func(c *Config) { c.Enabled = enabled } }

// WithServiceName sets the service name.
func WithServiceName(name string) Option { return func(c *Config) { c.ServiceName = name } }

// WithEndpoint sets the OTLP endpoint.
func WithEndpoint(ep string) Option { return func(c *Config) { c.Endpoint = ep } }

// WithProtocol sets the OTLP protocol.
func WithProtocol(p Protocol) Option { return func(c *Config) { c.Protocol = p } }

// Tracer wraps the configured TracerProvider and implements lifecycle.Component.
type Tracer struct {
	cfg      Config
	provider trace.TracerProvider
	// shutdown flushes and stops exporters; nil for the no-op (disabled) path.
	shutdown func(context.Context) error
	// setGlobals installs the provider + propagator as OTel globals on Start.
	setGlobals func()
}

// New constructs a Tracer. With Enabled=false it returns a no-op Tracer. With
// Enabled=true and a non-empty Endpoint it builds an OTLP exporter (grpc or
// http), a batch span processor, a parent-based ratio sampler and a resource
// describing the service. With Enabled=true and an empty Endpoint it builds the
// same SDK provider without an exporter (spans/ids generated, nothing sent).
func New(ctx context.Context, cfg Config, opts ...Option) (*Tracer, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if !cfg.Enabled {
		np := noop.NewTracerProvider()
		return &Tracer{
			cfg:        cfg,
			provider:   np,
			shutdown:   func(context.Context) error { return nil },
			setGlobals: func() { otel.SetTracerProvider(np) },
		}, nil
	}

	if cfg.ServiceName == "" {
		return nil, errors.New("tracing: ServiceName is required when tracing is enabled")
	}

	// Empty endpoint: keep the SDK provider (spans/ids still generated for log
	// correlation) but build no exporter, so nothing is sent over the network.
	if cfg.Endpoint == "" {
		return newSDKTracer(cfg, nil), nil
	}

	exp, err := newExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("tracing: build exporter: %w", err)
	}

	return newSDKTracer(cfg, exp), nil
}

// newSDKTracer builds an SDK-backed Tracer around a span exporter. It is the
// shared core used by New (with an OTLP exporter) and by tests (with an
// in-memory exporter), so the provider/sampler/resource wiring is exercised
// without a live collector. A nil exporter yields a provider that still creates
// spans (valid trace_id/span_id for log correlation) but exports nothing.
func newSDKTracer(cfg Config, exp sdktrace.SpanExporter) *Tracer {
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.DeploymentEnvironment(cfg.Environment),
	)

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	}
	if exp != nil {
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp, sdktrace.WithExportTimeout(cfg.ExportTimeout)))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)

	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	return &Tracer{
		cfg:      cfg,
		provider: tp,
		shutdown: tp.Shutdown,
		setGlobals: func() {
			otel.SetTracerProvider(tp)
			otel.SetTextMapPropagator(propagator)
		},
	}
}

func newExporter(ctx context.Context, cfg Config) (*otlptrace.Exporter, error) {
	switch cfg.Protocol {
	case ProtocolHTTP:
		opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
		}
		return otlptracehttp.New(ctx, opts...)
	case ProtocolGRPC, "":
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
		if cfg.Insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlptracegrpc.WithHeaders(cfg.Headers))
		}
		return otlptracegrpc.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("tracing: unknown protocol %q", cfg.Protocol)
	}
}

// Provider returns the configured TracerProvider (for otelgin and manual spans).
func (t *Tracer) Provider() trace.TracerProvider { return t.provider }

// Config returns the resolved configuration.
func (t *Tracer) Config() Config { return t.cfg }

// Name implements lifecycle.Component.
func (t *Tracer) Name() string { return "tracing" }

// Start installs the provider and propagator as OTel globals. It does not block.
func (t *Tracer) Start(context.Context) error {
	if t.setGlobals != nil {
		t.setGlobals()
	}
	return nil
}

// Stop flushes and shuts down the TracerProvider, honoring ctx's deadline.
func (t *Tracer) Stop(ctx context.Context) error {
	if t.shutdown == nil {
		return nil
	}
	return t.shutdown(ctx)
}
