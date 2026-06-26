# `pkg/log`

A zap-based structured logger with a human-friendly console format of `[time][LEVEL] message field=value ...` (or JSON). It is context-aware: a request-scoped logger carrying the OTel trace id and span id can be stored in and retrieved from a `context.Context`. Trace and span ids are read from the OpenTelemetry trace API (not the SDK), so this package never imports `pkg/tracing` and there is no import cycle.

## Import
```go
import "github.com/tuanlao/pulse/pkg/log"
```

## Configuration
| YAML key (`mapstructure` tag) | Type | Default (from `DefaultConfig`) | Description |
| --- | --- | --- | --- |
| `level` | `string` | `"info"` | Minimum enabled level: `debug`, `info`, `warn`, `error`, `dpanic`, `panic`, `fatal`. |
| `encoding` | `string` | `"console"` | `"console"` (bracketed human format) or `"json"`. |
| `development` | `bool` | `false` | Development-friendly settings (stacktraces on warn+). |
| `output_paths` | `[]string` | `["stderr"]` | Zap log sinks. |
| `error_output_paths` | `[]string` | `["stderr"]` | Zap internal-error sinks. |
| `trace_field` | `string` | `"trace_id"` | Log key for the OTel trace id. |
| `span_field` | `string` | `"span_id"` | Log key for the OTel span id. |

## Usage
```go
logger, err := log.New(log.DefaultConfig(), log.WithLevel("debug"))
if err != nil {
    panic(err)
}
defer logger.Sync()

logger.Info("server started", zap.Int("port", 8080))

// Request-scoped logging: attach trace/span ids from ctx.
reqLog := logger.ForContext(ctx)
ctx = log.IntoContext(ctx, reqLog)

// Later, in a handler deep down the stack:
log.FromContext(ctx, logger).Warn("slow query", zap.Duration("took", d))
```

## API / Options
- `New(cfg Config, opts ...Option) (*Logger, error)` — build a logger.
- `Nop() *Logger` — discards everything; safe default.
- `(*Logger).Debug/Info/Warn/Error(msg string, f ...zap.Field)` — leveled logging.
- `(*Logger).With(fields ...zap.Field) *Logger` — child logger with permanent fields.
- `(*Logger).ForContext(ctx) *Logger` — attach trace/span ids as fields.
- `(*Logger).Sync() error` — flush buffers.
- `(*Logger).Zap() *zap.Logger` / `(*Logger).Config() Config` — access underlying logger / resolved config.
- `(*Logger).LifecycleAdapter()` / `(*Logger).GocronAdapter()` — adapters for foreign key/value variadic logger interfaces (`lifecycle.Logger`, `gocron.Logger`). Both log through the underlying zap logger directly so the single caller-skip frame still points at the real call site.
- `IntoContext(ctx, *Logger) context.Context` / `FromContext(ctx, fallback *Logger) *Logger` — store/retrieve a request-scoped logger.
- Options: `WithLevel`, `WithEncoding`, `WithFieldNames(trace, span string)`.

## Notes
- Console format is `[time][LEVEL] msg field=value`; time is wrapped in brackets (`[2006-01-02T15:04:05.000Z0700]`) and the level uses a bracketed capital encoder.
- `ForContext` reads the trace/span ids from the OTel trace API. If ctx carries no valid span context it returns the receiver unchanged.
- `FromContext` never returns nil: it falls back to the supplied logger, or to `Nop()` if that is also nil.
- `Sync` ignores the harmless `invalid argument`, `inappropriate ioctl for device` and `bad file descriptor` errors that console/stderr sinks return on some platforms.
- A caller-skip of 1 frame is built in so the logged caller is your call site, not this package's wrapper methods.
