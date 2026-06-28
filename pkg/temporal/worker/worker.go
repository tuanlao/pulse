package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/tuanlao/pulse/pkg/log"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/sysinfo"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// Deps are the worker's collaborators.
type Deps struct {
	// Logger logs lifecycle events; nil falls back to a no-op logger.
	Logger *log.Logger
	// Client is the SHARED Temporal client the worker runs on. REQUIRED when
	// enabled. It is NOT owned — the worker never closes it (the client component
	// does).
	Client sdkclient.Client
}

// Worker wraps a Temporal SDK worker and implements lifecycle.Component.
type Worker struct {
	cfg      Config
	log      *log.Logger
	w        sdkworker.Worker
	disabled bool

	mu      sync.Mutex
	started bool
}

// Disabled returns a no-op worker whose lifecycle and registration methods do
// nothing. Registering it in the lifecycle manager is safe.
func Disabled(logger *log.Logger) *Worker {
	if logger == nil {
		logger = log.Nop()
	}
	return &Worker{cfg: DefaultConfig(), log: logger, disabled: true}
}

// New builds the worker from a shared client. It errors if the client is missing
// or the task queue is empty, and validates the resource-tuner config.
func New(cfg Config, deps Deps, opts ...Option) (*Worker, error) {
	cfg.ApplyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.ApplyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}
	if deps.Client == nil {
		return nil, errors.New("temporal worker: Deps.Client is required (build it with pkg/temporal/client)")
	}
	if cfg.TaskQueue == "" {
		return nil, errors.New("temporal worker: TaskQueue is required")
	}

	wopts, err := buildWorkerOptions(cfg)
	if err != nil {
		return nil, err
	}

	return &Worker{
		cfg: cfg,
		log: deps.Logger,
		w:   sdkworker.New(deps.Client, cfg.TaskQueue, wopts),
	}, nil
}

// buildWorkerOptions translates Config into sdkworker.Options, choosing between
// the resource-based tuner and the static concurrency caps (they are mutually
// exclusive in the SDK). It is a pure function so the OOM-control logic is unit
// testable without a live server.
func buildWorkerOptions(cfg Config) (sdkworker.Options, error) {
	opts := sdkworker.Options{
		StickyScheduleToStartTimeout:     cfg.Sticky.ScheduleToStartTimeout,
		DeadlockDetectionTimeout:         cfg.DeadlockDetectionTimeout,
		WorkerStopTimeout:                cfg.StopTimeout,
		EnableSessionWorker:              cfg.EnableSessionWorker,
		MaxConcurrentWorkflowTaskPollers: cfg.Concurrency.MaxConcurrentWorkflowTaskPollers,
		MaxConcurrentActivityTaskPollers: cfg.Concurrency.MaxConcurrentActivityTaskPollers,
	}

	if cfg.ResourceTuner.Enabled {
		if cfg.ResourceTuner.TargetMemory <= 0 || cfg.ResourceTuner.TargetCPU <= 0 {
			return sdkworker.Options{}, fmt.Errorf(
				"temporal worker: resource_tuner requires target_memory and target_cpu in (0,1], got mem=%v cpu=%v",
				cfg.ResourceTuner.TargetMemory, cfg.ResourceTuner.TargetCPU)
		}
		// Mutually exclusive with the static MaxConcurrent*ExecutionSize caps — set
		// only the tuner; the SDK errors if both are provided.
		tuner, err := sdkworker.NewResourceBasedTuner(sdkworker.ResourceBasedTunerOptions{
			TargetMem:            cfg.ResourceTuner.TargetMemory,
			TargetCpu:            cfg.ResourceTuner.TargetCPU,
			InfoSupplier:         sysinfo.SysInfoProvider(),
			ActivityRampThrottle: cfg.ResourceTuner.ActivityRampThrottle,
			WorkflowRampThrottle: cfg.ResourceTuner.WorkflowRampThrottle,
		})
		if err != nil {
			return sdkworker.Options{}, fmt.Errorf("temporal worker: build resource tuner: %w", err)
		}
		opts.Tuner = tuner
		return opts, nil
	}

	opts.MaxConcurrentWorkflowTaskExecutionSize = cfg.Concurrency.MaxConcurrentWorkflowTaskExecutionSize
	opts.MaxConcurrentActivityExecutionSize = cfg.Concurrency.MaxConcurrentActivityExecutionSize
	opts.MaxConcurrentLocalActivityExecutionSize = cfg.Concurrency.MaxConcurrentLocalActivityExecutionSize
	return opts, nil
}

// RegisterWorkflow registers a workflow function. Call before Start. No-op when
// disabled.
func (w *Worker) RegisterWorkflow(workflow interface{}) {
	if w.disabled {
		return
	}
	w.w.RegisterWorkflow(workflow)
}

// RegisterWorkflowWithOptions registers a workflow with explicit options.
func (w *Worker) RegisterWorkflowWithOptions(wf interface{}, options workflow.RegisterOptions) {
	if w.disabled {
		return
	}
	w.w.RegisterWorkflowWithOptions(wf, options)
}

// RegisterActivity registers an activity (function or struct of methods). Call
// before Start. No-op when disabled.
func (w *Worker) RegisterActivity(activity interface{}) {
	if w.disabled {
		return
	}
	w.w.RegisterActivity(activity)
}

// RegisterActivityWithOptions registers an activity with explicit options.
func (w *Worker) RegisterActivityWithOptions(a interface{}, options activity.RegisterOptions) {
	if w.disabled {
		return
	}
	w.w.RegisterActivityWithOptions(a, options)
}

// SDK returns the underlying Temporal SDK worker (escape hatch). Nil when
// disabled.
func (w *Worker) SDK() sdkworker.Worker {
	if w.disabled {
		return nil
	}
	return w.w
}

// Config returns the resolved configuration.
func (w *Worker) Config() Config { return w.cfg }

// Name implements lifecycle.Component.
func (w *Worker) Name() string { return "temporal-worker" }

// Start begins polling. It is non-blocking (the SDK runs its own poller
// goroutines). No-op when disabled.
func (w *Worker) Start(context.Context) error {
	if w.disabled {
		return nil
	}
	if err := w.w.Start(); err != nil {
		return fmt.Errorf("temporal worker: start: %w", err)
	}
	w.mu.Lock()
	w.started = true
	w.mu.Unlock()
	return nil
}

// Stop drains in-flight tasks and stops the worker, honoring ctx's deadline. The
// shared client is never closed here.
func (w *Worker) Stop(ctx context.Context) error {
	if w.disabled {
		return nil
	}
	// Flip started to false under the lock so a second (or concurrent) Stop is a
	// no-op and never calls the SDK's Stop twice.
	w.mu.Lock()
	started := w.started
	w.started = false
	w.mu.Unlock()
	if !started {
		return nil
	}

	// w.Stop blocks until in-flight tasks drain (bounded by WorkerStopTimeout).
	// Run it in a goroutine so we can also honor the lifecycle ctx deadline.
	done := make(chan struct{})
	go func() {
		w.w.Stop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
