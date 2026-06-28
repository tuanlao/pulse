// Package temporal is the facade of pulse's Temporal.io support for saga /
// distributed transactions, with Temporal as the orchestrator. It composes three
// focused sub-packages behind two constructors:
//
//   - client — a lifecycle.Component wrapping the Temporal SDK client (start /
//     signal / query workflows). A service that only orchestrates needs just this.
//   - worker — a lifecycle.Component that executes workflows/activities. It holds
//     the memory/OOM controls: the process-global sticky workflow cache size,
//     static concurrency caps, and an opt-in resource-based tuner.
//   - saga  — the in-workflow saga (compensating-transaction) helper plus the
//     Continue-As-New history guard (the headline lever against ballooning history).
//
// The facade dials ONE connection (with the OTel tracing interceptor and the
// tally→Prometheus metrics handler wired in) and the client component owns it;
// the worker is built on the SAME connection via Deps.SDKClient (shared, not
// owned). Public saga types are re-exported as aliases so callers import only
// "pkg/temporal".
package temporal

import (
	"fmt"
	"io"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/temporal/client"
	"github.com/tuanlao/pulse/pkg/temporal/internal/tclient"
	"github.com/tuanlao/pulse/pkg/temporal/saga"
	"github.com/tuanlao/pulse/pkg/temporal/worker"
	"go.opentelemetry.io/otel/trace"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.uber.org/zap"
)

// Re-exported public types so callers depend only on this package.
type (
	// ConnectionConfig is the Temporal connection config (host, namespace, TLS).
	ConnectionConfig = tclient.ConnectionConfig
	// Client is the Temporal client component.
	Client = client.Client
	// Worker is the Temporal worker component.
	Worker = worker.Worker
	// Saga is the in-workflow saga helper.
	Saga = saga.Saga
	// Thresholds bound a single workflow run's history (Continue-As-New guard).
	Thresholds = saga.Thresholds
)

// Re-exported saga helpers.
var (
	// NewSaga binds a Saga to a workflow context.
	NewSaga = saga.New
	// ShouldContinueAsNew reports whether a workflow should Continue-As-New.
	ShouldContinueAsNew = saga.ShouldContinueAsNew
	// DefaultThresholds returns the default history bounds.
	DefaultThresholds = saga.DefaultThresholds
)

// Deps are the facade's collaborators. All are nil-safe (degrade to no-op).
type Deps struct {
	// Logger logs lifecycle events and backs the SDK logger; nil -> no-op.
	Logger *log.Logger
	// TracerProvider supplies the tracer for the OTel interceptor; nil -> no-op
	// (no spans). Only used when Config.Tracing.Enabled.
	TracerProvider trace.TracerProvider
	// Registry is pulse's shared Prometheus registry; the SDK's metrics are
	// reported into it via tally when Config.Metrics.Enabled. Nil disables the
	// metrics bridge.
	Registry *prometheus.Registry
	// SDKClient is required by NewWorker: pass NewClient(...).SDK() so the worker
	// runs on the same connection (it does not own it).
	SDKClient sdkclient.Client
}

// stickyMu guards the process-global sticky workflow cache size.
var (
	stickyMu      sync.Mutex
	stickyApplied int
)

// NewClient dials the Temporal connection once (wiring the tracing interceptor
// and the tally→Prometheus metrics handler) and returns the client component,
// which owns and closes the connection on Stop. When Config.Enabled is false it
// returns a disabled no-op client.
func NewClient(cfg Config, deps Deps, opts ...Option) (*Client, error) {
	cfg.applyDefaults()
	for _, o := range opts {
		o(&cfg)
	}
	cfg.applyDefaults()

	if !cfg.Enabled {
		return client.Disabled(deps.Logger), nil
	}

	var (
		mh     sdkclient.MetricsHandler
		closer io.Closer
	)
	if cfg.Metrics.Enabled && deps.Registry != nil {
		var err error
		mh, closer, err = tclient.PrometheusMetricsHandler(deps.Registry, cfg.Metrics.Prefix)
		if err != nil {
			return nil, err
		}
	}

	return client.New(cfg.Client, client.Deps{
		Logger:         deps.Logger,
		TracerProvider: deps.TracerProvider,
		EnableTracing:  cfg.Tracing.Enabled,
		MetricsHandler: mh,
		MetricsCloser:  closer,
		Connection:     cfg.Connection,
	})
}

// NewWorker builds the worker component on the shared connection (Deps.SDKClient,
// from NewClient(...).SDK()). It applies the process-global sticky cache size once.
// When Config.Enabled or Config.Worker.Enabled is false it returns a disabled
// no-op worker.
func NewWorker(cfg Config, deps Deps, opts ...Option) (*Worker, error) {
	cfg.applyDefaults()
	for _, o := range opts {
		o(&cfg)
	}
	cfg.applyDefaults()

	if !cfg.Enabled || !cfg.Worker.Enabled {
		return worker.Disabled(deps.Logger), nil
	}
	if deps.SDKClient == nil {
		return nil, fmt.Errorf("temporal: NewWorker requires Deps.SDKClient (pass NewClient(...).SDK())")
	}

	// The sticky workflow cache size is PROCESS-GLOBAL in the SDK and must be set
	// before any worker starts. Apply the first non-zero request; warn on conflicts.
	applyStickyCacheSize(cfg.Worker.Sticky.CacheSize, deps.Logger)

	return worker.New(cfg.Worker, worker.Deps{
		Logger: deps.Logger,
		Client: deps.SDKClient,
	})
}

func applyStickyCacheSize(size int, logger *log.Logger) {
	if size <= 0 {
		return
	}
	stickyMu.Lock()
	defer stickyMu.Unlock()
	if stickyApplied == 0 {
		sdkworker.SetStickyWorkflowCacheSize(size)
		stickyApplied = size
		return
	}
	if stickyApplied != size && logger != nil {
		logger.Warn("temporal: sticky workflow cache size is process-global and already set; ignoring different value",
			zap.Int("applied", stickyApplied), zap.Int("requested", size))
	}
}
