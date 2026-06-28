# `pkg/grpc/server/interceptor`

The gRPC server interceptors pulse's `pkg/grpc/server` chains together, plus its package-owned RED metrics. Mirrors `pkg/http/server/middleware` for gRPC.

## Import
```go
import "github.com/tuanlao/pulse/pkg/grpc/server/interceptor"
```

## Interceptors
- **`RecoveryUnary(base)` / `RecoveryStream(base)`** — recover a panicking handler, log it (with `log.FromContext`, the value rendered `%+v` so `fmt.Formatter` panics keep their context, plus the stack) and return `status.Error(codes.Internal, ...)`. Outermost in the chain.
- **`ContextLoggerUnary(base)` / `ContextLoggerStream(base)`** — derive a request-scoped logger carrying the OTel `trace_id`/`span_id` (established by the otelgrpc StatsHandler) and store it in the context so handlers can retrieve it via `log.FromContext`. The stream variant wraps `grpc.ServerStream` so `ss.Context()` returns the logger-carrying context.

The server wires them as `Recovery → ContextLogger → Metrics` (outermost → innermost) via `grpc.ChainUnaryInterceptor`/`ChainStreamInterceptor`.

## Metrics
`Metrics` is the gRPC server RED collector set (a grpc-specific shape, like `pkg/kafka/metrics` and the HTTP client's `ClientMetrics` — not `pkg/metrics.RED`, whose labels are gin-shaped).

**`metrics.*`** (`MetricsConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics.enabled` | `bool` | `true` | Toggles metrics. When false, `NewMetrics` returns `nil`. |
| `metrics.namespace` | `string` | `"pulse"` | Prometheus namespace. |
| `metrics.subsystem` | `string` | `"grpc_server"` | Prometheus subsystem. |
| `metrics.buckets` | `[]float64` | `0.001 … 10` | Handler duration histogram buckets (seconds). |

Collectors (labels `grpc_service`, `grpc_method`, `grpc_type`, `grpc_code`):
- `..._handler_duration_seconds` (histogram)
- `..._handled_total` (counter)
- `..._in_flight` (gauge; labels without `grpc_code`)

```go
m, err := interceptor.NewMetrics(cfg.Metrics, sharedRegistry) // nil when disabled
// pass m to server.Deps{Metrics: m}
```

## API
- `NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*Metrics, error)` — registers into `reg` (or a fresh one); returns `nil` when disabled. `(*Metrics).Registry()`, `(*Metrics).Unary()`, `(*Metrics).Stream()`.
- `DefaultMetricsConfig() MetricsConfig`; `(*MetricsConfig).ApplyDefaults(d)`.

## Notes
- **Bounded cardinality:** `grpc_service`/`grpc_method` come from the static proto full method (split via the internal `splitFullMethod`, which collapses any malformed input to `unknown`); `grpc_type` is the streaming kind (`unary`/`server_stream`/`client_stream`/`bidi`); `grpc_code` is `codes.Code.String()`. No peer/message/metadata values are ever used as labels.
- **Panics are counted:** the metrics interceptor records under a `defer` (code `Internal` on a panic) then re-raises, so the panic is still converted + logged by the outer Recovery — the same pattern as the HTTP RED middleware.
- `Registry()` is for **exposing** the metrics (scrape), not runtime `Gather`.
