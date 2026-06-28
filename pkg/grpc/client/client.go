package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/tuanlao/pulse/pkg/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Deps are optional collaborators. Nil collaborators degrade gracefully.
type Deps struct {
	// Logger is the base logger for outbound call logs; nil disables logging.
	Logger *log.Logger
	// Metrics enables outbound RED metrics; nil disables them.
	Metrics *ClientMetrics
	// TracerProvider, when a real (exporting) provider, makes the client create
	// client spans via otelgrpc. nil or a no-op provider uses manual id injection.
	TracerProvider trace.TracerProvider
	// Propagator injects the W3C trace context. Default propagation.TraceContext{}.
	Propagator propagation.TextMapPropagator
}

// Client is a configurable outbound gRPC client owning a *grpc.ClientConn. It
// implements lifecycle.Component (Stop closes the connection).
type Client struct {
	cfg     Config
	deps    Deps
	conn    *grpc.ClientConn
	tracing bool
}

// New builds a Client and its (lazy) *grpc.ClientConn. grpc.NewClient does not
// connect eagerly; the connection is established on the first RPC (with
// background reconnection handled by gRPC).
func New(cfg Config, deps Deps, opts ...Option) (*Client, error) {
	return newClient(cfg, deps, nil, opts...)
}

// newClient is the shared constructor; extra dial options are appended last (used
// by tests to inject a bufconn dialer).
func newClient(cfg Config, deps Deps, extra []grpc.DialOption, opts ...Option) (*Client, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}
	if deps.Propagator == nil {
		deps.Propagator = propagation.TraceContext{}
	}
	if cfg.Target == "" {
		return nil, errors.New("grpc client: target is required")
	}

	c := &Client{cfg: cfg, deps: deps, tracing: isRealProvider(deps.TracerProvider)}
	dialOpts, err := c.buildDialOptions()
	if err != nil {
		return nil, err
	}
	dialOpts = append(dialOpts, extra...)

	conn, err := grpc.NewClient(cfg.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("grpc client: new client: %w", err)
	}
	c.conn = conn
	return c, nil
}

// buildDialOptions assembles the dial options: credentials, message-size caps,
// keepalive, retry service config, otelgrpc tracing and the interceptor chain.
func (c *Client) buildDialOptions() ([]grpc.DialOption, error) {
	cfg := c.cfg
	creds, err := buildClientTLS(cfg.TLS)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithUserAgent(cfg.UserAgent),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(cfg.MaxSendMsgSize),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                cfg.Keepalive.Time,
			Timeout:             cfg.Keepalive.Timeout,
			PermitWithoutStream: cfg.Keepalive.PermitWithoutStream,
		}),
	}

	if sc := cfg.serviceConfigJSON(); sc != "" {
		opts = append(opts, grpc.WithDefaultServiceConfig(sc))
	}

	unary := []grpc.UnaryClientInterceptor{obsUnary(c.deps.Metrics, c.deps.Logger)}
	stream := []grpc.StreamClientInterceptor{obsStream(c.deps.Metrics, c.deps.Logger)}

	if c.tracing {
		// otelgrpc owns the client span + W3C traceparent (the modern StatsHandler
		// API; the interceptor form is deprecated). Only the request-timeout
		// interceptor is added; the custom x-trace-id header is intentionally not
		// emitted on this path (traceparent already carries the trace id).
		opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(c.deps.TracerProvider),
			otelgrpc.WithPropagators(c.deps.Propagator),
		)))
		unary = append(unary, requestTimeoutUnary(cfg.Timeouts.Request))
	} else {
		// No real tracer: generate a trace id, bound the call, inject x-trace-id.
		unary = append(unary,
			ensureIDsUnary(),
			requestTimeoutUnary(cfg.Timeouts.Request),
			correlationUnary(cfg.Trace),
		)
		stream = append(stream, ensureIDsStream(), correlationStream(cfg.Trace))
	}

	opts = append(opts,
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithChainStreamInterceptor(stream...),
	)
	return opts, nil
}

// buildClientTLS returns insecure credentials when TLS is disabled, otherwise a
// TLS credential using the system roots (or a custom CA file) and an optional
// client certificate for mTLS. CAFile is optional: empty means system roots.
func buildClientTLS(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if !cfg.Enabled {
		return insecure.NewCredentials(), nil
	}
	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.ServerNameOverride,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // opt-in seam, default false
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("grpc client: read ca file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("grpc client: invalid ca file %q", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("grpc client: load client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

// isRealProvider reports whether tp is a real (exporting) provider rather than
// the OTel no-op, detecting the no-op by type to avoid a throwaway probe span.
func isRealProvider(tp trace.TracerProvider) bool {
	if tp == nil {
		return false
	}
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return false
	}
	return true
}

// Name implements lifecycle.Component.
func (c *Client) Name() string { return "grpc-client" }

// Start is a no-op: grpc.NewClient is lazy and connects on the first RPC, with
// background reconnection handled by gRPC. Forcing a connect here would only slow
// startup, raise spurious warnings, and duplicate gRPC's reconnect logic.
func (c *Client) Start(context.Context) error { return nil }

// Stop closes the underlying connection.
func (c *Client) Stop(context.Context) error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// CheckReady implements lifecycle.ReadinessChecker. Only a Shutdown connection is
// reported not-ready; Idle/Connecting/TransientFailure are all usable (RPCs
// connect or retry), so they do not fail readiness — that would needlessly mark
// the pod NotReady while gRPC reconnects in the background.
func (c *Client) CheckReady(context.Context) error {
	if c.conn.GetState() == connectivity.Shutdown {
		return errors.New("grpc client: connection shut down")
	}
	return nil
}

// Conn returns the underlying *grpc.ClientConn so callers can build typed stubs.
func (c *Client) Conn() *grpc.ClientConn { return c.conn }

// Config returns the resolved configuration.
func (c *Client) Config() Config { return c.cfg }
