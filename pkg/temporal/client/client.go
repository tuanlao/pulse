package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/temporal/internal/tclient"
	"go.opentelemetry.io/otel/trace"
	sdkclient "go.temporal.io/sdk/client"
)

// ErrDisabled is returned by helpers on a disabled client.
var ErrDisabled = errors.New("temporal: client disabled")

// Deps are the client's collaborators. All are nil-safe.
type Deps struct {
	// Logger logs lifecycle events and backs the SDK logger; nil falls back to a
	// no-op logger.
	Logger *log.Logger
	// TracerProvider supplies the tracer for the OTel interceptor; nil (or
	// EnableTracing false) means no interceptor.
	TracerProvider trace.TracerProvider
	// EnableTracing gates whether the OTel tracing interceptor is added when
	// dialing this client's own connection.
	EnableTracing bool
	// MetricsHandler is the SDK metrics handler set at dial time (e.g. the
	// tally→Prometheus bridge built by the facade). Nil disables SDK metrics.
	MetricsHandler sdkclient.MetricsHandler
	// MetricsCloser is closed on Stop (flushes the metrics scope). Nil is fine.
	MetricsCloser io.Closer
	// SDKClient injects an already-dialed connection to share. When set the client
	// does NOT own it (Stop never closes it). When nil the client dials its own
	// connection from Connection and owns it.
	SDKClient sdkclient.Client
	// Connection is used only when SDKClient is nil (the client dials + owns it).
	Connection tclient.ConnectionConfig
}

// Client wraps the Temporal SDK client and implements lifecycle.Component. The
// embedded sdkclient.Client promotes ExecuteWorkflow/SignalWorkflow/QueryWorkflow
// etc. directly onto *Client. Do NOT issue those calls on a disabled client (when
// Config.Enabled is false the embedded client is nil and a promoted call would
// panic) — guard with Enabled(), or use StartWorkflow, which returns ErrDisabled.
// This mirrors pkg/redis's embedded-rueidis disabled-client semantics.
type Client struct {
	sdkclient.Client
	cfg           Config
	log           *log.Logger
	metricsCloser io.Closer
	owned         bool
	disabled      bool
}

// Disabled returns a no-op client whose lifecycle methods do nothing and whose
// helpers return ErrDisabled. Registering it in the lifecycle manager is safe.
func Disabled(logger *log.Logger) *Client {
	if logger == nil {
		logger = log.Nop()
	}
	return &Client{cfg: DefaultConfig(), log: logger, disabled: true}
}

// New builds the client. When Deps.SDKClient is set it wraps that shared
// connection (not owned); otherwise it builds its own (lazy — no connection until
// Start's health check) from Deps.Connection and owns it (closing it on Stop).
func New(cfg Config, deps Deps, opts ...Option) (*Client, error) {
	cfg.ApplyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.ApplyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}

	c := &Client{cfg: cfg, log: deps.Logger, metricsCloser: deps.MetricsCloser}

	if deps.SDKClient != nil {
		c.Client = deps.SDKClient
		c.owned = false
		return c, nil
	}

	var tracer trace.Tracer
	if deps.EnableTracing && deps.TracerProvider != nil {
		tracer = deps.TracerProvider.Tracer("github.com/tuanlao/pulse/pkg/temporal/client")
	}

	sc, err := tclient.Build(tclient.BuildParams{
		Conn:           deps.Connection,
		Tracer:         tracer,
		MetricsHandler: deps.MetricsHandler,
		Logger:         deps.Logger,
	})
	if err != nil {
		if deps.MetricsCloser != nil {
			_ = deps.MetricsCloser.Close()
		}
		return nil, err
	}
	c.Client = sc
	c.owned = true
	return c, nil
}

// StartWorkflow starts a workflow, filling unset StartWorkflowOptions fields from
// the client's defaults (task queue, run/task timeouts).
func (c *Client) StartWorkflow(ctx context.Context, opts sdkclient.StartWorkflowOptions, workflow interface{}, args ...interface{}) (sdkclient.WorkflowRun, error) {
	if c.disabled {
		return nil, ErrDisabled
	}
	if opts.TaskQueue == "" {
		opts.TaskQueue = c.cfg.DefaultTaskQueue
	}
	if opts.WorkflowRunTimeout == 0 {
		opts.WorkflowRunTimeout = c.cfg.DefaultWorkflowRunTimeout
	}
	if opts.WorkflowTaskTimeout == 0 {
		opts.WorkflowTaskTimeout = c.cfg.DefaultWorkflowTaskTimeout
	}
	return c.Client.ExecuteWorkflow(ctx, opts, workflow, args...)
}

// SDK returns the underlying Temporal SDK client (escape hatch; also used to
// share the connection with a worker). It is nil for a disabled client.
func (c *Client) SDK() sdkclient.Client {
	if c.disabled {
		return nil
	}
	return c.Client
}

// Config returns the resolved configuration.
func (c *Client) Config() Config { return c.cfg }

// Enabled reports whether the client is active (not the disabled no-op).
func (c *Client) Enabled() bool { return !c.disabled }

// Name implements lifecycle.Component.
func (c *Client) Name() string { return "temporal-client" }

// Start verifies the server is reachable (only for a connection this client
// owns; a shared connection is health-checked by its owner). It is non-blocking
// beyond the bounded health check and a no-op when disabled.
func (c *Client) Start(ctx context.Context) error {
	if c.disabled || !c.owned {
		return nil
	}
	if _, err := c.Client.CheckHealth(ctx, &sdkclient.CheckHealthRequest{}); err != nil {
		return fmt.Errorf("temporal client: health check: %w", err)
	}
	return nil
}

// Stop closes the connection this client owns (never a shared/injected one) and
// flushes the metrics scope. It honors ctx implicitly (Close is fast).
func (c *Client) Stop(context.Context) error {
	if c.disabled {
		return nil
	}
	if c.owned && c.Client != nil {
		c.Client.Close()
	}
	if c.metricsCloser != nil {
		_ = c.metricsCloser.Close()
	}
	return nil
}

// CheckReady implements lifecycle.ReadinessChecker.
func (c *Client) CheckReady(ctx context.Context) error {
	if c.disabled {
		return nil
	}
	_, err := c.Client.CheckHealth(ctx, &sdkclient.CheckHealthRequest{})
	return err
}
