# pkg/temporal/saga

The **saga** (compensating-transaction) primitive for Temporal workflows, plus the
**Continue-As-New history guard**. This runs INSIDE workflow code, so everything
here is deterministic — it uses only the `go.temporal.io/sdk/workflow` API (no wall
clock, rand, real mutexes, or direct Prometheus calls). It is a leaf library (no
`lifecycle.Component`); import it from your workflow functions.

The Temporal Go SDK has **no built-in Saga type** (unlike the Java SDK); this is
pulse's reusable implementation.

## Saga API

| Symbol | Purpose |
|--------|---------|
| `New(ctx, opts...) *Saga` | bind a saga to the workflow context |
| `(*Saga) AddCompensation(c Compensation)` | record a raw undo closure (runs LIFO) |
| `(*Saga) AddActivityCompensation(activity, args...)` | record "run this undo activity" with the saga's default activity options |
| `(*Saga) AddActivityCompensationWithOptions(ao, activity, args...)` | same, with explicit activity options |
| `(*Saga) Compensate(ctx) error` | run the recorded compensations (idempotent) |
| `(*Saga) Run(ctx, fn) error` | run `fn`; on error, Compensate and return `errors.Join(fnErr, compErr)` |

Options (functional): `WithParallel(bool)`, `WithContinueOnError(bool)`,
`WithActivityOptions(workflow.ActivityOptions)`. Defaults: **reverse (LIFO) +
continue-on-error**, with bounded undo activities (30s + 3 retries).

```go
func TransferSaga(ctx workflow.Context, in transferInput) error {
    ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
    s := saga.New(ctx)
    return s.Run(ctx, func() error {
        if err := workflow.ExecuteActivity(ctx, Debit, in.From).Get(ctx, nil); err != nil { return err }
        s.AddActivityCompensation(Credit, in.From) // undo the debit
        if err := workflow.ExecuteActivity(ctx, Credit, in.To).Get(ctx, nil); err != nil { return err }
        s.AddActivityCompensation(Debit, in.To)    // undo the credit
        return nil
    }) // an error here means the completed steps were already compensated in reverse
}
```

## History guard (Continue-As-New)

| Symbol | Purpose |
|--------|---------|
| `Thresholds` | history bounds (`max_events`, `max_bytes`, `ignore_server_suggestion`); also a config struct |
| `DefaultThresholds()` | `10000` events / `20 MiB`, respecting the server's CAN suggestion |
| `ShouldContinueAsNew(ctx, Thresholds) bool` | true when the run should Continue-As-New to keep history bounded |

A long-running / looping workflow accumulates event history forever; large
histories slow replay and grow worker memory until it OOMs. Only a workflow can
Continue-As-New, so the guard only **signals** — the author makes the call:

```go
for in.Remaining > 0 {
    // ... process one unit of work ...
    in.Remaining--
    if in.Remaining > 0 && saga.ShouldContinueAsNew(ctx, in.Thresholds) {
        return workflow.NewContinueAsNewError(ctx, MyWorkflow, in) // carry state forward
    }
}
```

## Notes

- Pass `Thresholds` into the workflow **input** (from config) so they stay
  deploy-time constant — keeping the guard deterministic across replays.
- `Continue-As-New` only carries the state you pass to `NewContinueAsNewError`;
  thread the minimum needed to resume.
- In **parallel** mode (`WithParallel(true)`) compensations run via `workflow.Go`
  and all run regardless of individual failures; reverse-sequential (the default)
  honors `ContinueOnError`.
- When the guard fires it bumps `pulse_saga_continue_as_new_suggested` on the
  workflow metrics handler (which rides the same tally→Prometheus bridge as the SDK
  metrics).
