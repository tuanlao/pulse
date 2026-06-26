// Package server provides a configurable gin-based HTTP server that plugs into
// the pulse lifecycle: it implements lifecycle.Component, sets safe http.Server
// timeouts (including ReadHeaderTimeout against Slowloris), wires the standard
// middleware chain (recovery, tracing, request logger, RED metrics, CORS, body
// limit) and exposes separate /healthz (liveness) and /readyz
// (readiness) endpoints backed by a ReadinessRegistry.
package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/http/server/middleware"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
	"github.com/tuanlao/pulse/pkg/version"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/trace"
)

// TLSConfig is a seam for future TLS support.
type TLSConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
}

// GzipConfig is a seam for future response compression.
type GzipConfig struct {
	Enabled bool `mapstructure:"enabled"`
	Level   int  `mapstructure:"level"`
}

// Config configures the HTTP server. Every field has a default.
type Config struct {
	// Port is the TCP port to listen on (binds all interfaces, ":<port>").
	// Default 8080.
	Port int `mapstructure:"port"`
	// Mode is the gin mode: "release", "debug" or "test". Default "release".
	Mode string `mapstructure:"mode"`

	// ReadTimeout is the max duration for reading the entire request. Default 15s.
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	// WriteTimeout is the max duration before timing out writes. Default 15s.
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	// IdleTimeout is the max keep-alive idle time. Default 60s.
	IdleTimeout time.Duration `mapstructure:"idle_timeout"`
	// ReadHeaderTimeout caps header reads (Slowloris guard). Default 5s.
	ReadHeaderTimeout time.Duration `mapstructure:"read_header_timeout"`
	// ShutdownDrain bounds in-flight draining on Stop. Should be <= the lifecycle
	// ShutdownTimeout. Default 25s.
	ShutdownDrain time.Duration `mapstructure:"shutdown_drain"`

	// MaxBodyBytes caps request body size. Default 1 MiB. Non-positive disables.
	MaxBodyBytes int64 `mapstructure:"max_body_bytes"`

	// HealthzPath is the liveness endpoint. Default "/healthz".
	HealthzPath string `mapstructure:"healthz_path"`
	// ReadyzPath is the readiness endpoint. Default "/readyz".
	ReadyzPath string `mapstructure:"readyz_path"`
	// ReadinessCheckTimeout bounds each readiness check. Default 2s.
	ReadinessCheckTimeout time.Duration `mapstructure:"readiness_check_timeout"`

	// CORS configures cross-origin handling (disabled by default).
	CORS middleware.CORSConfig `mapstructure:"cors"`

	// TLS and Gzip are seams reserved for later phases.
	TLS  TLSConfig  `mapstructure:"tls"`
	Gzip GzipConfig `mapstructure:"gzip"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Port:                  8080,
		Mode:                  gin.ReleaseMode,
		ReadTimeout:           15 * time.Second,
		WriteTimeout:          15 * time.Second,
		IdleTimeout:           60 * time.Second,
		ReadHeaderTimeout:     5 * time.Second,
		ShutdownDrain:         25 * time.Second,
		MaxBodyBytes:          1 << 20,
		HealthzPath:           "/healthz",
		ReadyzPath:            "/readyz",
		ReadinessCheckTimeout: 2 * time.Second,
		CORS:                  middleware.DefaultCORSConfig(),
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Port == 0 {
		c.Port = d.Port
	}
	if c.Mode == "" {
		c.Mode = d.Mode
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = d.ReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = d.IdleTimeout
	}
	if c.ReadHeaderTimeout <= 0 {
		c.ReadHeaderTimeout = d.ReadHeaderTimeout
	}
	if c.ShutdownDrain <= 0 {
		c.ShutdownDrain = d.ShutdownDrain
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = d.MaxBodyBytes
	}
	if c.HealthzPath == "" {
		c.HealthzPath = d.HealthzPath
	}
	if c.ReadyzPath == "" {
		c.ReadyzPath = d.ReadyzPath
	}
	if c.ReadinessCheckTimeout <= 0 {
		c.ReadinessCheckTimeout = d.ReadinessCheckTimeout
	}
}

// listenAddr returns the ":<port>" address the server binds to.
func (c *Config) listenAddr() string { return ":" + strconv.Itoa(c.Port) }

// Option overrides Config fields.
type Option func(*Config)

// WithPort sets the listen port.
func WithPort(port int) Option { return func(c *Config) { c.Port = port } }

// WithMode sets the gin mode.
func WithMode(mode string) Option { return func(c *Config) { c.Mode = mode } }

// WithCORS overrides the CORS config.
func WithCORS(cors middleware.CORSConfig) Option { return func(c *Config) { c.CORS = cors } }

// Deps are the cross-cutting collaborators the server wires into its middleware.
// All are optional except Logger; nil collaborators disable their middleware.
type Deps struct {
	// Logger is required; nil falls back to a no-op logger.
	Logger *log.Logger
	// Metrics, when non-nil, enables the RED middleware and mounts the scrape
	// endpoint at its configured path.
	Metrics *metrics.RED
	// TracerProvider, when non-nil, enables otelgin span instrumentation.
	TracerProvider trace.TracerProvider
	// ServiceName labels otelgin spans. Default "pulse-http".
	ServiceName string
	// OnServeError, when non-nil, is called if the background Serve goroutine
	// exits with a non-ErrServerClosed error (a fatal serve failure after a
	// successful bind). The composition root typically wires this to cancel the
	// lifecycle manager's context so the process shuts down instead of running on
	// with a dead listener. When nil the error is only logged.
	OnServeError func(error)
}

// Server is a configurable gin HTTP server implementing lifecycle.Component.
type Server struct {
	cfg     Config
	deps    Deps
	engine  *gin.Engine
	httpSrv *http.Server
	ready   *ReadinessRegistry
	logger  *log.Logger
}

// New constructs a Server, building the gin engine, middleware chain, health
// endpoints and the underlying http.Server with safe timeouts.
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
		deps.ServiceName = "pulse-http"
	}

	gin.SetMode(cfg.Mode)
	engine := gin.New()

	s := &Server{
		cfg:    cfg,
		deps:   deps,
		engine: engine,
		ready:  NewReadinessRegistry(cfg.ReadinessCheckTimeout),
		logger: deps.Logger,
	}

	s.installMiddleware()
	s.installHealthRoutes()
	s.installMetricsRoute()

	s.httpSrv = &http.Server{
		Addr:              cfg.listenAddr(),
		Handler:           engine,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	return s, nil
}

// installMiddleware wires the chain from outermost to innermost.
func (s *Server) installMiddleware() {
	e := s.engine

	// Outermost: recovery catches everything inside.
	e.Use(middleware.Recovery(s.logger))
	// OTel span instrumentation (only when a provider is supplied).
	if s.deps.TracerProvider != nil {
		e.Use(otelgin.Middleware(s.deps.ServiceName, otelgin.WithTracerProvider(s.deps.TracerProvider)))
	}
	// Request-scoped logger (after tracing so the trace/span ids are present).
	e.Use(middleware.ContextLogger(s.logger))
	// RED metrics (route-pattern labels), skipping observability/health paths.
	if s.deps.Metrics != nil {
		e.Use(s.deps.Metrics.Middleware(s.skipPaths()...))
	}
	// CORS (pass-through when disabled).
	e.Use(middleware.CORS(s.cfg.CORS))
	// Body size limit.
	e.Use(middleware.BodyLimit(s.cfg.MaxBodyBytes))
}

// skipPaths lists endpoints excluded from RED metrics.
func (s *Server) skipPaths() []string {
	paths := []string{s.cfg.HealthzPath, s.cfg.ReadyzPath}
	if s.deps.Metrics != nil {
		paths = append(paths, s.deps.Metrics.Config().Path)
	}
	return paths
}

// installHealthRoutes mounts separate liveness and readiness endpoints.
func (s *Server) installHealthRoutes() {
	s.engine.GET(s.cfg.HealthzPath, func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"version": version.Get(),
		})
	})

	s.engine.GET(s.cfg.ReadyzPath, func(c *gin.Context) {
		results, ready := s.ready.Evaluate(c.Request.Context())
		code := http.StatusOK
		status := "ready"
		if !ready {
			code = http.StatusServiceUnavailable
			status = "not_ready"
		}
		c.JSON(code, gin.H{
			"status": status,
			"checks": results,
		})
	})
}

// installMetricsRoute mounts the Prometheus scrape endpoint when metrics are
// enabled.
func (s *Server) installMetricsRoute() {
	if s.deps.Metrics == nil {
		return
	}
	s.engine.GET(s.deps.Metrics.Config().Path, gin.WrapH(s.deps.Metrics.Handler()))
}

// Engine returns the gin engine so the application can register its routes.
func (s *Server) Engine() *gin.Engine { return s.engine }

// Readiness returns the readiness registry so components can register checks.
func (s *Server) Readiness() *ReadinessRegistry { return s.ready }

// Config returns the resolved configuration.
func (s *Server) Config() Config { return s.cfg }

// Name implements lifecycle.Component. Register the server LAST so it is the
// first thing stopped on shutdown.
func (s *Server) Name() string { return "http" }

// Start binds the listener synchronously (so bind errors surface to the
// lifecycle manager) and serves in a background goroutine. It does not block.
func (s *Server) Start(context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.listenAddr())
	if err != nil {
		return err
	}
	go func() {
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("http server error: " + err.Error())
			// Surface the failure so the lifecycle manager can shut down instead of
			// running on with a dead listener. Falls back to log-only when unwired.
			if s.deps.OnServeError != nil {
				s.deps.OnServeError(err)
			}
		}
	}()
	return nil
}

// Stop gracefully shuts the server down: it stops accepting new connections and
// drains in-flight requests within the smaller of ctx's deadline and
// ShutdownDrain.
func (s *Server) Stop(ctx context.Context) error {
	drainCtx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownDrain)
	defer cancel()
	return s.httpSrv.Shutdown(drainCtx)
}
