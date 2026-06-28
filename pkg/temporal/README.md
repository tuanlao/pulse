# pkg/temporal

Saga / distributed transactions for pulse services, orchestrated by
[Temporal.io](https://temporal.io). It wraps the Temporal Go SDK behind pulse's
conventions (composable `lifecycle.Component`s, `Config` + `DefaultConfig()` +
`Option`s, nil-safe `Deps`, OTel tracing, shared Prometheus registry) and adds
the bits the SDK leaves to you: a reusable **saga** (compensating-transaction)
helper and first-class **controls so workflow history cannot balloon and OOM the
worker**.

It is split into focused sub-packages (composable, not all-or-nothing):

| Package | Purpose |
| --- | --- |
| `temporal` (facade) | Aliases + `NewClient` / `NewWorker`; dials one connection and wires tracing + metrics. |
| `temporal/client` | `lifecycle.Component` wrapping the SDK client — start/signal/query workflows. Owns the connection. |
| `temporal/worker` | `lifecycle.Component` that executes workflows/activities. **Holds the memory/OOM controls.** |
| `temporal/saga` | In-workflow saga helper + the Continue-As-New history guard. No lifecycle (runs inside workflow code). |

A service that only **starts** workflows needs just `client`; one that **executes**
them also needs `worker`. The worker is built on the **same** connection the
client dials (shared, not owned).

```go
import "github.com/tuanlao/pulse/pkg/temporal"
```

## Why two history controls

Temporal records an event history per workflow run. Long-running or looping
workflows accumulate history forever; large histories slow replay and grow worker
memory until it OOMs. pulse gives you both levers:

1. **Continue-As-New (CAN) guard** (`temporal/saga`, config `history:`) — bounds a
   single run's history. Only a workflow can CAN, so the package provides
   `ShouldContinueAsNew(ctx, thresholds)` and you make the call. **This is the
   primary fix for ballooning history.**
2. **Worker memory controls** (`temporal/worker`, config `worker:`) — the
   process-global **sticky cache size**, **static concurrency caps**, and an
   opt-in **resource-based tuner** that throttles slots to keep memory under a
   target. These bound how much history/state is held in RAM at once.

## Configuration

Top-level (`temporal:`):

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `false` | Gates the whole subsystem. When false `NewClient`/`NewWorker` return safe no-ops. |
| `connection` | object | — | Frontend connection, shared by client + worker (see below). |
| `client` | object | — | Client behavior defaults (see below). |
| `worker` | object | — | Worker + **OOM controls** (see below). |
| `history` | object | — | Continue-As-New thresholds (see below). |
| `metrics.enabled` | bool | `true` | Bridge the SDK's own metrics into the shared registry via tally. |
| `metrics.prefix` | string | `temporal` | Metric name prefix for SDK metrics. |
| `tracing.enabled` | bool | `true` | Add the OTel tracing interceptor. |

`connection.*`:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `host_port` | string | `localhost:7233` | Temporal frontend address. |
| `namespace` | string | `default` | Temporal namespace. |
| `identity` | string | `""` | Client identity; empty lets the SDK derive `pid@host`. |
| `tls.enabled` | bool | `false` | Enable TLS. `tls.ca_file`/`cert_file`/`key_file`/`server_name`/`insecure_skip_verify` configure CA verification and mutual TLS. |

`client.*`:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `default_task_queue` | string | `""` | Task queue `StartWorkflow` uses when unset by the caller. |
| `default_workflow_run_timeout` | duration | `0` | Default run timeout (0 = unbounded). |
| `default_workflow_task_timeout` | duration | `10s` | Default workflow-task timeout. |

`worker.*` — **the OOM control surface**:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | Toggle the worker independently of the client. |
| `task_queue` | string | — | **Required when enabled.** Queue the worker polls. |
| `stop_timeout` | duration | `30s` | Bounds graceful shutdown (in-flight task draining). |
| `deadlock_detection_timeout` | duration | `1s` | Per-workflow-task deadlock guard. |
| `enable_session_worker` | bool | `false` | Activity sessions (mutually exclusive with the resource tuner). |
| `sticky.cache_size` | int | `10000` | **PROCESS-GLOBAL** number of workflow histories cached in RAM. The primary OOM lever. First non-zero value wins across all workers in the process. |
| `sticky.schedule_to_start_timeout` | duration | `5s` | Sticky task schedule-to-start timeout. |
| `concurrency.max_concurrent_workflow_task_execution_size` | int | `1000` | Cap on in-flight workflow tasks. Used only when the tuner is off. |
| `concurrency.max_concurrent_activity_execution_size` | int | `1000` | Cap on in-flight activities. Used only when the tuner is off. |
| `concurrency.max_concurrent_local_activity_execution_size` | int | `1000` | Cap on in-flight local activities. Used only when the tuner is off. |
| `concurrency.max_concurrent_workflow_task_pollers` | int | `2` | Workflow-task pollers. |
| `concurrency.max_concurrent_activity_task_pollers` | int | `2` | Activity-task pollers. |
| `resource_tuner.enabled` | bool | `false` | Dynamically throttle slots to keep memory/CPU under target. **Mutually exclusive** with the `concurrency.*` execution caps. |
| `resource_tuner.target_memory` | float | `0.8` | Target system memory usage (0,1]. Must be > 0 when enabled. |
| `resource_tuner.target_cpu` | float | `0.9` | Target system CPU usage (0,1]. Must be > 0 when enabled. |
| `resource_tuner.activity_ramp_throttle` | duration | `50ms` | How fast activity slots ramp up. |
| `resource_tuner.workflow_ramp_throttle` | duration | `0` | How fast workflow slots ramp up. |

`history.*` — Continue-As-New guard:

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `max_events` | int | `10000` | CAN when the run's history event count reaches this. |
| `max_bytes` | int | `20971520` (20 MiB) | CAN when the run's history size reaches this. |
| `ignore_server_suggestion` | bool | `false` | When false, also CAN when the Temporal server suggests it (as history nears the server-side limits). |

## Usage

Wire the client once and share its connection with the worker (see
`examples/service/main.go`):

```go
tcli, _ := temporal.NewClient(cfg.Temporal, temporal.Deps{
    Logger:         logger,
    TracerProvider: tracer.Provider(),
    Registry:       red.Registry(), // SDK metrics → shared /metrics via tally
})
tworker, _ := temporal.NewWorker(cfg.Temporal, temporal.Deps{
    Logger:    logger,
    SDKClient: tcli.SDK(), // shared connection (not owned by the worker)
})
tworker.RegisterWorkflow(TransferSaga)
tworker.RegisterActivity(Debit)
tworker.RegisterActivity(Credit)

// Register client first, worker after → on shutdown the worker drains, then the
// client closes the connection.
mgr.Register(tcli)
mgr.Register(tworker)

// Start a saga from anywhere holding the client:
run, _ := tcli.StartWorkflow(ctx, client.StartWorkflowOptions{TaskQueue: "tq"}, TransferSaga, input)
```

### Saga (compensating transactions)

`saga.Saga` records a compensation after each successful step and runs them on
failure — by default in reverse (LIFO) order, continuing past individual failures
(best-effort cleanup). Both are configurable (`WithParallel`,
`WithContinueOnError`, `WithActivityOptions`).

```go
func TransferSaga(ctx workflow.Context, in transferInput) error {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
    s := saga.New(ctx)
    return s.Run(ctx, func() error {
        if err := workflow.ExecuteActivity(ctx, Debit, in.From, in.Amount).Get(ctx, nil); err != nil {
            return err
        }
        s.AddActivityCompensation(Credit, in.From, in.Amount) // undo the debit
        if err := workflow.ExecuteActivity(ctx, Credit, in.To, in.Amount).Get(ctx, nil); err != nil {
            return err
        }
        s.AddActivityCompensation(Debit, in.To, in.Amount)    // undo the credit
        return nil
    }) // an error here means the completed steps were already compensated in reverse
}
```

### Continue-As-New guard (bounding history)

Only a workflow can Continue-As-New; the guard tells you when to. Pass the
configured thresholds into the workflow input so they stay deploy-time constant
(deterministic across replays):

```go
func OrderSagaWorkflow(ctx workflow.Context, in orderBatchInput) error {
    for in.Remaining > 0 {
        // ... process one unit of work ...
        in.Remaining--
        if in.Remaining > 0 && saga.ShouldContinueAsNew(ctx, in.Thresholds) {
            return workflow.NewContinueAsNewError(ctx, OrderSagaWorkflow, in) // carry state forward
        }
    }
    return nil
}
```

## API / Options / Deps

- **Facade**: `NewClient(cfg, deps, opts...)`, `NewWorker(cfg, deps, opts...)`;
  aliases `Client`, `Worker`, `Saga`, `Thresholds`, `ConnectionConfig`; helpers
  `NewSaga`, `ShouldContinueAsNew`, `DefaultThresholds`. Options: `WithEnabled`,
  `WithHostPort`, `WithNamespace`, `WithTaskQueue`.
- **Deps** (all nil-safe): `Logger` (→ no-op), `TracerProvider` (→ no-op),
  `Registry` (nil disables the SDK metrics bridge), `SDKClient` (required by
  `NewWorker` — pass `NewClient(...).SDK()`).
- **client**: `StartWorkflow`, `SDK()`, `CheckReady`, plus everything promoted from
  the embedded SDK client (`ExecuteWorkflow`, `SignalWorkflow`, `QueryWorkflow`, …).
- **worker**: `RegisterWorkflow(WithOptions)`, `RegisterActivity(WithOptions)`,
  `SDK()`. Options: `WithTaskQueue`, `WithStickyCacheSize`, `WithResourceTuner`,
  `WithMaxConcurrentActivities`.
- **saga**: `New`, `AddCompensation`, `AddActivityCompensation(WithOptions)`,
  `Compensate`, `Run`; `Thresholds`, `DefaultThresholds`, `ShouldContinueAsNew`.

## Notes

- **Sticky cache size is PROCESS-GLOBAL** in the Temporal SDK. The facade applies
  the first non-zero value once (`sync.Once`-style) and logs a warning if another
  worker requests a different size (it is ignored).
- **Resource tuner ⇄ static caps are mutually exclusive.** When
  `resource_tuner.enabled` is true the `concurrency.*` execution caps are not set
  (the SDK errors if both are provided); poller counts still apply. The tuner pulls
  in `go.temporal.io/sdk/contrib/sysinfo` (gopsutil); it is off by default.
- **Determinism**: everything in `saga` uses only the `workflow.*` API (no wall
  clock, rand, real mutexes, or direct Prometheus calls). Continue-As-New only
  carries state you pass into `NewContinueAsNewError` — thread the minimum needed
  to resume.
- **Metrics**: the SDK's own metrics (request counts/latencies, sticky-cache size,
  …) are bridged into pulse's shared registry via `tally` under the `temporal`
  prefix, so `/metrics` exposes them alongside the rest. The CAN guard bumps
  `pulse_saga_continue_as_new_suggested` on the same pipeline.
- **Tracing**: the OTel tracing interceptor is set on the connection and
  propagates spans across client → workflow → activity automatically; because it
  is a combined interceptor it also covers workers built from that client.
- **Shutdown order**: register the client before the worker so the worker drains
  in-flight tasks first, then the client closes the connection.
- Run a local server for the demo with `temporal server start-dev` (frontend
  `:7233`, UI `:8233`) and set `temporal.enabled: true`.
