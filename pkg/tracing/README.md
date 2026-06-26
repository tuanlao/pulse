# `pkg/tracing`

Configures an OpenTelemetry `TracerProvider` that exports spans over OTLP (gRPC or HTTP). Tracing is enabled by default. The `Tracer` implements `lifecycle.Component`: `Start` installs the provider and propagator as OTel globals, and `Stop` flushes and shuts the provider down. When disabled, `New` returns a `Tracer` backed by a no-op provider so callers can wire it unconditionally.

### Three states

| `enabled` | `endpoint` | Result |
| --- | --- | --- |
| `true` | non-empty | Real SDK provider; spans exported to the OTLP collector (full tracing). |
| `true` | `""` (empty) | Real SDK provider; spans **generated** (so `trace_id`/`span_id` reach the logs) but **not exported** — no collector needed. |
| `false` | — | No-op provider: no spans, no trace id. |

The empty-endpoint state — which is the **default** — is the way to get trace ids in your logs without running a collector. Set a non-empty `endpoint` (or `WithEndpoint("localhost:4317")`) to opt into exporting spans.

## Import
```go
import "github.com/tuanlao/pulse/pkg/tracing"
```

## Configuration
| YAML key (`mapstructure` tag) | Type | Default (from `DefaultConfig`) | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Toggles tracing. |
| `service_name` | `string` | `""` | OTel `service.name` resource attribute. **Required when enabled.** |
| `service_version` | `string` | `""` | `service.version` resource attribute. |
| `environment` | `string` | `""` | `deployment.environment` resource attribute. |
| `protocol` | `Protocol` | `"grpc"` | OTLP transport: `"grpc"` or `"http"`. |
| `endpoint` | `string` | `""` | OTLP collector endpoint (`:4317` grpc, `:4318` http). **Empty (the default) disables OTLP export** — tracing stays on and spans are still generated for log correlation; set a non-empty value to export. |
| `insecure` | `bool` | `true` | Disables transport security (plaintext); dev default. |
| `headers` | `map[string]string` | `nil` | Extra OTLP exporter headers (e.g. vendor auth tokens). |
| `sample_ratio` | `float64` | `1.0` | Parent-based ratio sampler in `[0,1]`. `0` is honored (never sample); a negative value means "unset" and is backfilled to the default. |
| `export_timeout` | `time.Duration` | `30s` | Bounds a single export. |

## Usage
```go
tracer, err := tracing.New(ctx, tracing.DefaultConfig(),
    tracing.WithServiceName("orders-api"))
if err != nil {
    panic(err)
}

// Via lifecycle, or manually:
_ = tracer.Start(ctx)       // installs OTel globals (provider + propagator)
defer tracer.Stop(ctx)      // flushes and shuts down the provider

tp := tracer.Provider()     // hand to otelgin / manual spans
```

## API / Options
- `New(ctx, cfg Config, opts ...Option) (*Tracer, error)` — build a tracer (no-op when disabled; errors if `ServiceName` is empty while enabled).
- `(*Tracer).Start(ctx) error` — install provider + composite (trace-context + baggage) propagator as globals.
- `(*Tracer).Stop(ctx) error` — flush and shut down, honoring ctx's deadline.
- `(*Tracer).Provider() trace.TracerProvider`, `Config() Config`, `Name() string` (`"tracing"`).
- `GenerateTraceID() trace.TraceID` / `GenerateSpanID() trace.SpanID` — random, valid ids.
- `WithGeneratedSpanContext(ctx) context.Context` — ensure ctx carries a valid span context.
- Constants: `ProtocolGRPC`, `ProtocolHTTP`.
- Options: `WithEnabled`, `WithServiceName`, `WithEndpoint`, `WithProtocol`.

## Notes
- Uses OTLP exporters (`otlptracegrpc` / `otlptracehttp`) with a batch span processor and a parent-based `TraceIDRatioBased` sampler. With an empty endpoint the provider is built **without** a batcher/exporter — spans are sampled and carry valid ids but are dropped instead of exported.
- `Stop` calls `TracerProvider.Shutdown`, which flushes pending spans — satisfying the "flush traces on shutdown" requirement.
- `WithGeneratedSpanContext` returns ctx unchanged if it already has a valid span context; otherwise it attaches a freshly generated, sampled span context. Useful for background workers (e.g. cron jobs) with no inbound request, so logs and propagation always have a trace id even when export is disabled.
- When `Enabled=false` you still get a working (no-op) `Tracer`, so downstream wiring need not branch on the flag.
