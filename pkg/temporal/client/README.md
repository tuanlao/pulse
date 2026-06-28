# pkg/temporal/client

The Temporal **client** component (`lifecycle.Component`). Wraps the Temporal SDK
client to start / signal / query / cancel workflows. A service that only
orchestrates (starts) sagas needs just this; one that executes them also needs
[`pkg/temporal/worker`](../worker/README.md). Normally built and wired by the
[`pkg/temporal`](../README.md) facade (`temporal.NewClient`), which dials one
connection that this component **owns** and closes on `Stop`.

The struct embeds `sdkclient.Client`, so all SDK methods (`ExecuteWorkflow`,
`SignalWorkflow`, `QueryWorkflow`, …) are promoted onto `*Client`.

## API

| Method | Purpose |
|--------|---------|
| `StartWorkflow(ctx, opts, wf, args...)` | start a workflow, filling unset `TaskQueue` / run+task timeouts from `Config`; returns `ErrDisabled` when disabled |
| `SDK() sdkclient.Client` | the underlying SDK client (also used to share the connection with a worker); `nil` when disabled |
| `Enabled() bool` | whether the client is active (not the disabled no-op) |
| `Start(ctx) error` | health-check the owned connection (bounded by the lifecycle start timeout); no-op when shared/disabled |
| `Stop(ctx) error` | close the owned connection + flush the metrics scope; never closes a shared connection |
| `CheckReady(ctx) error` | `lifecycle.ReadinessChecker` — server health check |

Plus everything promoted from the embedded `sdkclient.Client`.

## Config (`client`)

| Key | Default | Notes |
|-----|---------|-------|
| `default_task_queue` | `""` | task queue `StartWorkflow` uses when the caller leaves it empty |
| `default_workflow_run_timeout` | `0` | default run timeout (0 = unbounded) |
| `default_workflow_task_timeout` | `10s` | default workflow-task timeout |

The connection (host, namespace, identity, TLS) lives in the facade's shared
`connection:` block, not here.

## Notes

- The client is built **lazy** (`New` does not connect); connectivity is verified
  in `Start` via `CheckHealth`, bounded by the lifecycle start timeout — matching
  pulse's "cheap constructor, bounded I/O in Start" pattern (cf. `pkg/redis`).
- Disabled (`temporal.enabled: false`) → a no-op client: lifecycle methods and
  `StartWorkflow` are safe, but **do not call the promoted SDK methods on a
  disabled client** (the embedded client is nil and they would panic) — guard with
  `Enabled()`. Same semantics as `pkg/redis`'s embedded-rueidis disabled client.
