# `pkg/lifecycle`

The spine that sequences a service's long-lived components. Each component implements `Component` and is registered with a `Manager`, which owns OS-signal handling, ordered startup, reverse-ordered shutdown, a shutdown timeout, and error aggregation. There is deliberately no god constructor: the application's `main()` is the composition root and registers only the components it actually uses.

## Import
```go
import "github.com/tuanlao/pulse/pkg/lifecycle"
```

## Configuration

`Config` controls manager timing and signal handling. Defaults come from `DefaultConfig()`.

| YAML key (`mapstructure`) | Type | Default | Description |
| --- | --- | --- | --- |
| `shutdown_timeout` | `time.Duration` | `30s` | Bounds the entire shutdown sequence. |
| `start_timeout` | `time.Duration` | `15s` | Bounds the entire startup sequence. |
| `-` (`Signals`) | `[]os.Signal` | `SIGINT, SIGTERM` | Signals that trigger shutdown. Not loaded from config. |

## Usage
```go
mgr := lifecycle.New(lifecycle.DefaultConfig(), logger,
	lifecycle.WithShutdownTimeout(20*time.Second),
)

// Register in dependency order; HTTP server LAST so it stops FIRST.
mgr.Register(db, cache, httpServer)

if err := mgr.Run(ctx); err != nil {
	log.Fatal(err)
}
```

## API / Options
- `Component` — interface with `Name() string`, `Start(ctx) error`, `Stop(ctx) error`.
- `ReadinessChecker` — optional `CheckReady(ctx) error`, consumed by the HTTP readiness registry.
- `Logger` — minimal `Info`/`Error` surface so lifecycle need not import `pkg/log`.
- `New(cfg Config, log Logger, opts ...Option) *Manager` — constructs a manager (nil logger → no-op).
- `(*Manager).Register(comps ...Component)` — adds components in dependency order.
- `(*Manager).Run(ctx) error` — starts all, blocks until signal/cancel, then stops in reverse.
- `SafeGo(name string, fn func(), onPanic func(name string, recovered any, stack []byte))` — runs `fn` in a goroutine, recovering panics.
- Options: `WithShutdownTimeout(d)`, `WithStartTimeout(d)`, `WithSignals(sigs...)`.

## Notes
- Registration order = start order; shutdown runs in REVERSE order. Register the HTTP server last so it is the first thing stopped (stop accepting traffic before tearing down dependencies).
- A failed `Start` rolls back the components already started; `Run` aggregates any start/stop errors via `errors.Join`.
- `Start` MUST NOT block — bind synchronously, then serve in a goroutine; `Stop` must honor the context deadline.
- Panics in `Start`/`Stop` (and `SafeGo`) are recovered and converted to errors with a stack trace, so one bad component cannot crash the process mid-lifecycle.
