package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/tuanlao/pulse/pkg/grpc/server/interceptor"
	"github.com/tuanlao/pulse/pkg/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// Deps are the cross-cutting collaborators the server wires into its interceptor
// chain. All are optional except Logger; nil collaborators disable their layer.
type Deps struct {
	// Logger is required; nil falls back to a no-op logger.
	Logger *log.Logger
	// Metrics, when non-nil, adds the RED interceptors.
	Metrics *interceptor.Metrics
	// TracerProvider, when a real (exporting) provider, adds the otelgrpc
	// StatsHandler. nil or a no-op provider disables span instrumentation.
	TracerProvider trace.TracerProvider
	// ServiceName labels otelgrpc spans. Default "pulse-grpc".
	ServiceName string
	// OnServeError, when non-nil, is called if the background Serve goroutine exits
	// with a non-graceful error. The composition root typically wires this to
	// cancel the lifecycle manager's context. When nil the error is only logged.
	OnServeError func(error)
}

// Server is a configurable gRPC server implementing lifecycle.Component.
type Server struct {
	cfg     Config
	deps    Deps
	grpcSrv *grpc.Server
	health  *health.Server
	logger  *log.Logger

	mu       sync.Mutex
	started  bool
	listener net.Listener
}

// New constructs a Server, building the *grpc.Server with the interceptor chain,
// keepalive/message-size options, optional TLS credentials and otelgrpc tracing,
// and registering the health (default on) and reflection (opt-in) services.
func New(cfg Config, deps Deps, opts ...Option) (*Server, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}
	if deps.ServiceName == "" {
		deps.ServiceName = "pulse-grpc"
	}

	s := &Server{cfg: cfg, deps: deps, logger: deps.Logger}

	serverOpts, err := s.buildServerOptions()
	if err != nil {
		return nil, err
	}
	s.grpcSrv = grpc.NewServer(serverOpts...)

	if cfg.EnableHealth {
		s.health = health.NewServer()
		grpc_health_v1.RegisterHealthServer(s.grpcSrv, s.health)
		// Overall status starts NOT_SERVING until Start flips it to SERVING.
		s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	}
	if cfg.EnableReflection {
		reflection.Register(s.grpcSrv)
	}
	return s, nil
}

// buildServerOptions assembles the grpc.ServerOptions: message-size caps,
// keepalive, optional TLS, otelgrpc tracing (StatsHandler) and the interceptor
// chain.
func (s *Server) buildServerOptions() ([]grpc.ServerOption, error) {
	cfg := s.cfg
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(cfg.MaxSendMsgSize),
		grpc.ConnectionTimeout(cfg.ConnectionTimeout),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:                  cfg.Keepalive.Time,
			Timeout:               cfg.Keepalive.Timeout,
			MaxConnectionIdle:     cfg.Keepalive.MaxConnectionIdle,
			MaxConnectionAge:      cfg.Keepalive.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.Keepalive.MaxConnectionAgeGrace,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             cfg.Keepalive.EnforcementMinTime,
			PermitWithoutStream: cfg.Keepalive.EnforcementPermitWithoutStream,
		}),
	}

	if cfg.TLS.Enabled {
		creds, err := buildServerTLS(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}

	// Tracing via otelgrpc StatsHandler — the modern API (the interceptor form is
	// deprecated). It brackets the whole RPC, so the trace/span ids are on the
	// context before the ContextLogger interceptor runs.
	if isRealProvider(s.deps.TracerProvider) {
		opts = append(opts, grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithTracerProvider(s.deps.TracerProvider),
			otelgrpc.WithPropagators(propagation.TraceContext{}),
		)))
	}

	// Interceptor chain, outermost → innermost: recovery catches everything, then
	// the request-scoped logger, then metrics times only the handler.
	unary := []grpc.UnaryServerInterceptor{
		interceptor.RecoveryUnary(s.logger),
		interceptor.ContextLoggerUnary(s.logger),
	}
	stream := []grpc.StreamServerInterceptor{
		interceptor.RecoveryStream(s.logger),
		interceptor.ContextLoggerStream(s.logger),
	}
	if s.deps.Metrics != nil {
		unary = append(unary, s.deps.Metrics.Unary())
		stream = append(stream, s.deps.Metrics.Stream())
	}
	opts = append(opts,
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	)
	return opts, nil
}

// buildServerTLS loads the server keypair and (when ClientCAFile is set) the
// client CA pool for mTLS.
func buildServerTLS(cfg TLSConfig) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("grpc server: load tls keypair: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if cfg.ClientCAFile != "" {
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("grpc server: read client ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("grpc server: invalid client ca file %q", cfg.ClientCAFile)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

// isRealProvider reports whether tp is a real (exporting) provider rather than
// the OTel no-op. It detects the no-op by type so it does NOT create a throwaway
// probe span. Mirrors the HTTP client's helper.
func isRealProvider(tp trace.TracerProvider) bool {
	if tp == nil {
		return false
	}
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return false
	}
	return true
}

// Name implements lifecycle.Component. Register the server LAST so it is the
// first thing stopped on shutdown.
func (s *Server) Name() string { return "grpc" }

// Start binds the listener synchronously (so bind errors surface to the lifecycle
// manager) and serves in a background goroutine. It does not block.
func (s *Server) Start(context.Context) error {
	ln, err := net.Listen(s.cfg.Network, s.cfg.listenAddr())
	if err != nil {
		return err
	}
	return s.serve(ln)
}

// serve records the listener, flips health to SERVING and serves in a background
// goroutine. It is shared by Start and tests (which inject a bufconn listener).
func (s *Server) serve(ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.started = true
	s.mu.Unlock()

	if s.health != nil {
		s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	}
	go func() {
		if err := s.grpcSrv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			s.logger.Error("grpc server error: " + err.Error())
			if s.deps.OnServeError != nil {
				s.deps.OnServeError(err)
			}
		}
	}()
	return nil
}

// Stop gracefully drains in-flight RPCs within the smaller of ctx's deadline and
// ShutdownDrain, then hard-stops if the drain times out.
func (s *Server) Stop(ctx context.Context) error {
	if s.health != nil {
		s.health.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		// Shutdown marks every service NOT_SERVING so in-flight Watch streams see it.
		s.health.Shutdown()
	}
	drainCtx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownDrain)
	defer cancel()

	done := make(chan struct{})
	go func() {
		s.grpcSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-drainCtx.Done():
		s.grpcSrv.Stop() // hard stop cancels in-flight RPCs and unblocks GracefulStop
		<-done
		return drainCtx.Err()
	}
}

// CheckReady implements lifecycle.ReadinessChecker. It reports ready once the
// server has started serving.
func (s *Server) CheckReady(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return errors.New("grpc server: not started")
	}
	return nil
}

// Register applies fn against the underlying *grpc.Server to register a service
// implementation. It MUST be called before Start (gRPC requires all services
// registered before Serve); calling it after Start panics. A nil fn is ignored.
func (s *Server) Register(fn func(*grpc.Server)) {
	if fn == nil {
		return
	}
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		panic("grpc server: Register called after Start")
	}
	fn(s.grpcSrv)
}

// Server returns the underlying *grpc.Server (escape hatch for registration).
func (s *Server) Server() *grpc.Server { return s.grpcSrv }

// Health returns the gRPC health server (nil when health is disabled).
func (s *Server) Health() *health.Server { return s.health }

// Config returns the resolved configuration.
func (s *Server) Config() Config { return s.cfg }

// SetServingStatus sets the health serving status for a service ("" is the
// overall status). It is a no-op when health is disabled. Use it to gate a
// service SERVING only once its dependencies are ready.
func (s *Server) SetServingStatus(service string, serving bool) {
	if s.health == nil {
		return
	}
	st := grpc_health_v1.HealthCheckResponse_NOT_SERVING
	if serving {
		st = grpc_health_v1.HealthCheckResponse_SERVING
	}
	s.health.SetServingStatus(service, st)
}
