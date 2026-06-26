package redis

import (
	"context"
	"fmt"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidishook"
	"github.com/tuanlao/pulse/pkg/log"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Deps are optional collaborators. Nil collaborators degrade gracefully.
type Deps struct {
	// Logger logs lifecycle events; nil falls back to a no-op logger.
	Logger *log.Logger
	// TracerProvider creates a span per command; nil uses a no-op provider.
	// Spans are only created when Config.Tracing.Enabled.
	TracerProvider trace.TracerProvider
	// Metrics enables per-command Prometheus metrics; build it with NewMetrics
	// (sharing the server's registry) and pass it here. Nil disables metrics.
	Metrics *Metrics
}

// Client wraps a rueidis.Client and implements lifecycle.Component. The
// rueidis.Client is embedded, so all command methods (B, Do, DoCache,
// DoMultiCache, ...) are available directly on *Client.
type Client struct {
	rueidis.Client
	cfg      Config
	log      *log.Logger
	disabled bool
}

// New builds a redis Client. It maps Config to a rueidis.ClientOption (including
// client-side caching / broadcast tracking), dials the server, and — when
// tracing or metrics is active — wraps the client with an instrumentation hook.
//
// When Config.Enabled is false it returns a disabled Client that never dials and
// whose lifecycle methods are no-ops; do not issue commands on a disabled client.
func New(cfg Config, deps Deps, opts ...Option) (*Client, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}

	if !cfg.Enabled {
		return &Client{cfg: cfg, log: deps.Logger, disabled: true}, nil
	}

	opt, err := cfg.clientOption()
	if err != nil {
		return nil, err
	}
	base, err := rueidis.NewClient(opt)
	if err != nil {
		return nil, fmt.Errorf("redis: new client: %w", err)
	}

	var tracer trace.Tracer
	if cfg.Tracing.Enabled {
		tp := deps.TracerProvider
		if tp == nil {
			tp = noop.NewTracerProvider()
		}
		tracer = tp.Tracer("github.com/tuanlao/pulse/pkg/redis")
	}

	client := base
	if tracer != nil || deps.Metrics != nil {
		client = rueidishook.WithHook(base, &hook{tracer: tracer, m: deps.Metrics})
	}

	return &Client{Client: client, cfg: cfg, log: deps.Logger}, nil
}

// Config returns the resolved configuration.
func (c *Client) Config() Config { return c.cfg }

// Enabled reports whether this is a live client (Config.Enabled was true). A
// disabled client never dialed and its embedded rueidis.Client is nil, so
// issuing commands on it panics; callers that share the client (e.g. cron's
// distributed lock) should skip a disabled one.
func (c *Client) Enabled() bool { return !c.disabled }

// Name implements lifecycle.Component.
func (c *Client) Name() string { return "redis" }

// Start verifies connectivity with a PING (when ping_on_start is set) so a
// broken connection fails startup fast. It is a no-op when disabled. The dial
// itself happens in New; this is bounded setup work that honors ctx.
func (c *Client) Start(ctx context.Context) error {
	if c.disabled {
		return nil
	}
	if !c.cfg.PingOnStart {
		return nil
	}
	if err := c.Client.Do(ctx, c.Client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("redis: ping on start: %w", err)
	}
	return nil
}

// Stop closes the underlying client (synchronous in rueidis). It is a no-op when
// disabled.
func (c *Client) Stop(context.Context) error {
	if c.disabled || c.Client == nil {
		return nil
	}
	c.Client.Close()
	return nil
}

// CheckReady implements lifecycle.ReadinessChecker via a PING. A disabled client
// is always ready (it is intentionally not in use).
func (c *Client) CheckReady(ctx context.Context) error {
	if c.disabled {
		return nil
	}
	if err := c.Client.Do(ctx, c.Client.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("redis: not ready: %w", err)
	}
	return nil
}
