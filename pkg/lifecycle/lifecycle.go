// Package lifecycle is the spine that sequences a service's long-lived
// components. Each component implements Component and is registered with a
// Manager. The manager owns OS-signal handling, ordered startup, reverse-ordered
// shutdown, a shutdown timeout, and error aggregation.
//
// There is deliberately no god constructor: the application's main() is the
// composition root and registers only the components it actually uses.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// Component is the contract every long-lived subsystem implements.
type Component interface {
	// Name identifies the component in logs and aggregated errors.
	Name() string
	// Start begins serving or connecting. It MUST NOT block: a server should
	// bind its listener (returning any bind error synchronously) and then serve
	// in a goroutine. Start should honor ctx for any bounded setup work.
	Start(ctx context.Context) error
	// Stop performs graceful shutdown and must honor ctx's deadline.
	Stop(ctx context.Context) error
}

// ReadinessChecker is optionally implemented by components that gate readiness
// (e.g. /readyz). It is consumed by the HTTP layer's readiness registry.
type ReadinessChecker interface {
	CheckReady(ctx context.Context) error
}

// Logger is the minimal logging surface the manager needs. Keeping it tiny means
// lifecycle does not import pkg/log; the application passes an adapter (or a
// no-op).
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// nopLogger is used when no logger is supplied.
type nopLogger struct{}

func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// Config controls manager timing and signal handling.
type Config struct {
	// ShutdownTimeout bounds the entire shutdown sequence. Default 30s.
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	// StartTimeout bounds the entire startup sequence. Default 15s.
	StartTimeout time.Duration `mapstructure:"start_timeout"`
	// Signals trigger shutdown. Default SIGINT, SIGTERM. Not loaded from config.
	Signals []os.Signal `mapstructure:"-"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ShutdownTimeout: 30 * time.Second,
		StartTimeout:    15 * time.Second,
		Signals:         []os.Signal{syscall.SIGINT, syscall.SIGTERM},
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithShutdownTimeout sets the shutdown timeout.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Config) { c.ShutdownTimeout = d }
}

// WithStartTimeout sets the startup timeout.
func WithStartTimeout(d time.Duration) Option {
	return func(c *Config) { c.StartTimeout = d }
}

// WithSignals overrides the shutdown signals.
func WithSignals(sigs ...os.Signal) Option {
	return func(c *Config) { c.Signals = sigs }
}

// Manager registers components and runs their lifecycle.
type Manager struct {
	cfg    Config
	log    Logger
	mu     sync.Mutex
	comps  []Component
	notify func(context.Context, ...os.Signal) (context.Context, context.CancelFunc)
}

// New creates a Manager. A nil logger is replaced with a no-op.
func New(cfg Config, log Logger, opts ...Option) *Manager {
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = DefaultConfig().ShutdownTimeout
	}
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = DefaultConfig().StartTimeout
	}
	if len(cfg.Signals) == 0 {
		cfg.Signals = DefaultConfig().Signals
	}
	if log == nil {
		log = nopLogger{}
	}
	return &Manager{cfg: cfg, log: log, notify: signal.NotifyContext}
}

// Register adds components in dependency order. They start in registration order
// and stop in REVERSE order. Register the HTTP server LAST so it is the FIRST
// thing stopped (stop accepting traffic before tearing down its dependencies).
func (m *Manager) Register(comps ...Component) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.comps = append(m.comps, comps...)
}

// Run starts all components, blocks until a shutdown signal (or ctx cancel),
// then shuts everything down in reverse order. A failed Start rolls back the
// components already started. The returned error aggregates any Start failure or
// Stop errors via errors.Join.
func (m *Manager) Run(ctx context.Context) error {
	started, startErr := m.startAll(ctx)
	if startErr != nil {
		// Roll back whatever managed to start.
		stopErr := m.stopAll(started)
		return errors.Join(startErr, stopErr)
	}

	sigCtx, stop := m.notify(ctx, m.cfg.Signals...)
	defer stop()

	<-sigCtx.Done()
	m.log.Info("shutdown signal received, stopping components")

	return m.stopAll(started)
}

// startAll starts components in registration order, recovering from panics. It
// returns the slice of successfully started components (for rollback) and the
// first error encountered.
func (m *Manager) startAll(ctx context.Context) ([]Component, error) {
	m.mu.Lock()
	comps := append([]Component(nil), m.comps...)
	m.mu.Unlock()

	startCtx, cancel := context.WithTimeout(ctx, m.cfg.StartTimeout)
	defer cancel()

	started := make([]Component, 0, len(comps))
	for _, c := range comps {
		if err := safeStart(startCtx, c); err != nil {
			m.log.Error("component failed to start", "component", c.Name(), "error", err)
			return started, fmt.Errorf("starting %s: %w", c.Name(), err)
		}
		m.log.Info("component started", "component", c.Name())
		started = append(started, c)
	}
	return started, nil
}

// stopAll stops the given components in REVERSE order, each bounded by the
// shutdown timeout, aggregating errors.
func (m *Manager) stopAll(started []Component) error {
	if len(started) == 0 {
		return nil
	}
	shCtx, cancel := context.WithTimeout(context.Background(), m.cfg.ShutdownTimeout)
	defer cancel()

	var errs []error
	for i := len(started) - 1; i >= 0; i-- {
		c := started[i]
		if err := safeStop(shCtx, c); err != nil {
			m.log.Error("component failed to stop", "component", c.Name(), "error", err)
			errs = append(errs, fmt.Errorf("stopping %s: %w", c.Name(), err))
			continue
		}
		m.log.Info("component stopped", "component", c.Name())
	}
	return errors.Join(errs...)
}

// safeStart runs Start, converting a panic into an error so one bad component
// cannot crash the process mid-lifecycle.
func safeStart(ctx context.Context, c Component) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Start: %v\n%s", r, debug.Stack())
		}
	}()
	return c.Start(ctx)
}

// safeStop runs Stop with the same panic protection.
func safeStop(ctx context.Context, c Component) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Stop: %v\n%s", r, debug.Stack())
		}
	}()
	return c.Stop(ctx)
}

// SafeGo runs fn in a new goroutine, recovering from panics and reporting them
// through onPanic (which may be nil). Useful for long-running background loops
// that should not crash the process.
func SafeGo(name string, fn func(), onPanic func(name string, recovered any, stack []byte)) {
	go func() {
		defer func() {
			if r := recover(); r != nil && onPanic != nil {
				onPanic(name, r, debug.Stack())
			}
		}()
		fn()
	}()
}
