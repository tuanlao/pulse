// Package tclient holds the connection-level configuration (host, namespace,
// identity, TLS) shared by pulse's temporal client and worker, plus the helpers
// that turn it into a Temporal SDK client — including the OTel tracing
// interceptor and the tally→Prometheus metrics handler. It is internal: only
// pkg/temporal and its sub-packages compose it (the facade exposes ConnectionConfig
// as an alias).
package tclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tuanlao/pulse/pkg/log"
	tally "github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	sdkclient "go.temporal.io/sdk/client"
	contribotel "go.temporal.io/sdk/contrib/opentelemetry"
	contribtally "go.temporal.io/sdk/contrib/tally"
	"go.temporal.io/sdk/interceptor"
)

// ConnectionConfig is the Temporal connection configuration shared by the client
// and worker.
type ConnectionConfig struct {
	// HostPort is the Temporal frontend address (host:port). Default
	// "localhost:7233".
	HostPort string `mapstructure:"host_port"`
	// Namespace is the Temporal namespace this connection works with. Default
	// "default".
	Namespace string `mapstructure:"namespace"`
	// Identity overrides the client identity. Empty lets the SDK derive
	// pid@hostname. Default "".
	Identity string `mapstructure:"identity"`
	// TLS configures transport security. Opt-in.
	TLS TLSConfig `mapstructure:"tls"`
}

// TLSConfig configures TLS for the Temporal connection.
type TLSConfig struct {
	// Enabled toggles TLS. Default false.
	Enabled bool `mapstructure:"enabled"`
	// InsecureSkipVerify disables certificate verification (dev only).
	InsecureSkipVerify bool `mapstructure:"insecure_skip_verify"`
	// CAFile is a PEM bundle to verify the server certificate against.
	CAFile string `mapstructure:"ca_file"`
	// CertFile / KeyFile are the client certificate for mutual TLS.
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	// ServerName overrides the SNI / verification hostname.
	ServerName string `mapstructure:"server_name"`
}

// DefaultConnectionConfig returns the connection defaults.
func DefaultConnectionConfig() ConnectionConfig {
	return ConnectionConfig{
		HostPort:  "localhost:7233",
		Namespace: "default",
	}
}

// ApplyDefaults fills empty fields from DefaultConnectionConfig.
func (c *ConnectionConfig) ApplyDefaults() {
	d := DefaultConnectionConfig()
	if c.HostPort == "" {
		c.HostPort = d.HostPort
	}
	if c.Namespace == "" {
		c.Namespace = d.Namespace
	}
}

// BuildParams carries everything Build needs so connection config, tracing
// interceptor and metrics handler are composed in one place.
type BuildParams struct {
	Conn ConnectionConfig
	// Tracer, when non-nil, adds the OTel tracing interceptor (propagating spans
	// across client→workflow→activity boundaries).
	Tracer trace.Tracer
	// Propagator is the text-map propagator used by the tracing interceptor; nil
	// defaults to the W3C trace-context propagator.
	Propagator propagation.TextMapPropagator
	// MetricsHandler, when non-nil, is set as the SDK metrics handler.
	MetricsHandler sdkclient.MetricsHandler
	// Logger backs the SDK logger; nil falls back to the SDK default.
	Logger *log.Logger
}

// Build constructs a LAZY Temporal client: it does NOT connect to the server
// (matching pulse's "cheap constructor, bounded I/O in Start" pattern — like
// rueidis in pkg/redis). Connectivity is verified later by the component's Start
// via CheckHealth, which is bounded by the lifecycle StartTimeout. It only errors
// here on bad options (e.g. invalid TLS material).
func Build(p BuildParams) (sdkclient.Client, error) {
	p.Conn.ApplyDefaults()

	opts := sdkclient.Options{
		HostPort:  p.Conn.HostPort,
		Namespace: p.Conn.Namespace,
		Identity:  p.Conn.Identity,
	}
	if p.Logger != nil {
		opts.Logger = p.Logger.TemporalAdapter()
	}
	if p.MetricsHandler != nil {
		opts.MetricsHandler = p.MetricsHandler
	}
	if p.Conn.TLS.Enabled {
		tc, err := tlsConfig(p.Conn.TLS)
		if err != nil {
			return nil, err
		}
		opts.ConnectionOptions = sdkclient.ConnectionOptions{TLS: tc}
	}
	if p.Tracer != nil {
		ti, err := TracingInterceptor(p.Tracer, p.Propagator)
		if err != nil {
			return nil, err
		}
		opts.Interceptors = []interceptor.ClientInterceptor{ti}
	}

	c, err := sdkclient.NewLazyClient(opts)
	if err != nil {
		return nil, fmt.Errorf("temporal: build client for %s: %w", p.Conn.HostPort, err)
	}
	return c, nil
}

// TracingInterceptor builds the OTel tracing interceptor. The interceptor it
// returns implements the combined interceptor.Interceptor, so when set on the
// client it also intercepts workers built from that client.
func TracingInterceptor(tr trace.Tracer, prop propagation.TextMapPropagator) (interceptor.Interceptor, error) {
	if prop == nil {
		// Set explicitly so we don't depend on the OTel global being installed.
		prop = propagation.TraceContext{}
	}
	ti, err := contribotel.NewTracingInterceptor(contribotel.TracerOptions{
		Tracer:            tr,
		TextMapPropagator: prop,
	})
	if err != nil {
		return nil, fmt.Errorf("temporal: build tracing interceptor: %w", err)
	}
	return ti, nil
}

// PrometheusMetricsHandler builds an SDK metrics handler backed by a tally scope
// whose reporter registers into reg (pulse's shared registry) — so the SDK's own
// metrics (request counts/latencies, sticky-cache size, ...) appear under the
// given prefix on the shared /metrics endpoint. The returned io.Closer flushes
// and stops the tally scope; close it on shutdown.
func PrometheusMetricsHandler(reg *prometheus.Registry, prefix string) (sdkclient.MetricsHandler, io.Closer, error) {
	if prefix == "" {
		prefix = "temporal"
	}
	reporter := tallyprom.NewReporter(tallyprom.Options{Registerer: reg})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		Prefix:          prefix,
		Separator:       "_",
		CachedReporter:  reporter,
		SanitizeOptions: &contribtally.PrometheusSanitizeOptions,
	}, time.Second)
	scope = contribtally.NewPrometheusNamingScope(scope)
	return contribtally.NewMetricsHandler(scope), closer, nil
}

// tlsConfig builds a *tls.Config from the TLS settings: an optional CA bundle for
// server verification and an optional client certificate for mutual TLS.
func tlsConfig(c TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.InsecureSkipVerify,
		ServerName:         c.ServerName,
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("temporal: read tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("temporal: tls ca_file %q has no valid certificates", c.CAFile)
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("temporal: load tls client cert: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
