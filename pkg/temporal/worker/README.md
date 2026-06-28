# pkg/temporal/worker

The Temporal **worker** component (`lifecycle.Component`). Polls a task queue and
executes workflows and activities. Built on the shared Temporal client (passed via
`Deps.Client`, **never owned** — the client component closes it). Normally built
and wired by the [`pkg/temporal`](../README.md) facade (`temporal.NewWorker`).

**This is where the memory / OOM controls live.** (The other half of history
control — Continue-As-New — lives in [`pkg/temporal/saga`](../saga/README.md),
since only a workflow can call it.)

## API

| Method | Purpose |
|--------|---------|
| `RegisterWorkflow(wf)` / `RegisterWorkflowWithOptions(wf, opts)` | register a workflow (call before `Start`; no-op when disabled) |
| `RegisterActivity(a)` / `RegisterActivityWithOptions(a, opts)` | register an activity (function or struct of methods) |
| `SDK() sdkworker.Worker` | the underlying SDK worker; `nil` when disabled |
| `Start(ctx) error` | begin polling (non-blocking — the SDK runs its own pollers) |
| `Stop(ctx) error` | drain in-flight tasks and stop, honoring `ctx`'s deadline |

## Config (`worker`) — OOM controls

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | toggle the worker independently of the client |
| `task_queue` | — | **required when enabled** |
| `stop_timeout` | `30s` | bounds graceful shutdown (in-flight draining) |
| `deadlock_detection_timeout` | `1s` | per-workflow-task deadlock guard |
| `enable_session_worker` | `false` | activity sessions (mutually exclusive with the resource tuner) |
| `sticky.cache_size` | `10000` | **PROCESS-GLOBAL** count of workflow histories cached in RAM — the primary OOM lever; first non-zero value in the process wins |
| `sticky.schedule_to_start_timeout` | `5s` | sticky task schedule-to-start timeout |
| `concurrency.max_concurrent_workflow_task_execution_size` | `1000` | cap on in-flight workflow tasks (used only when the tuner is off) |
| `concurrency.max_concurrent_activity_execution_size` | `1000` | cap on in-flight activities (tuner off only) |
| `concurrency.max_concurrent_local_activity_execution_size` | `1000` | cap on in-flight local activities (tuner off only) |
| `concurrency.max_concurrent_workflow_task_pollers` | `2` | workflow-task pollers |
| `concurrency.max_concurrent_activity_task_pollers` | `2` | activity-task pollers |
| `resource_tuner.enabled` | `false` | dynamically throttle slots to keep memory/CPU under target; **mutually exclusive** with the `concurrency.*` execution caps |
| `resource_tuner.target_memory` | `0.8` | target system memory usage (0,1] |
| `resource_tuner.target_cpu` | `0.9` | target system CPU usage (0,1] |
| `resource_tuner.activity_ramp_throttle` | `50ms` | how fast activity slots ramp up |
| `resource_tuner.workflow_ramp_throttle` | `0` | how fast workflow slots ramp up |

## Notes

- **`sticky.cache_size` is process-global** in the Temporal SDK
  (`worker.SetStickyWorkflowCacheSize`); the facade applies the first non-zero
  value once and warns on a conflicting later value.
- The **resource tuner** (opt-in) pulls in `go.temporal.io/sdk/contrib/sysinfo`
  (gopsutil). When enabled the static `concurrency.*` execution caps are not set
  (the SDK errors if both are provided); poller counts still apply.
- The worker inherits the tracing interceptor and SDK metrics handler from the
  shared client connection — it does not configure them itself.
- Disabled (`temporal.enabled: false` or `worker.enabled: false`) → a no-op worker
  whose registration and lifecycle methods do nothing.
