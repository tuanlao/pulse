# `pkg/grpc/client`

A configurable outbound gRPC client for calling other services. It owns a `*grpc.ClientConn` (built with the modern, lazy `grpc.NewClient`), wires otelgrpc tracing via a **StatsHandler** (or manual `x-trace-id` correlation when no real tracer is present), applies a per-call request timeout, retry via the gRPC service config, keepalive, message-size caps and TLS, and exposes client-side RED metrics plus outbound logging. It implements `lifecycle.Component` so the manager closes the connection on shutdown. Like every pulse package it exposes `Config` + `DefaultConfig()` + functional `Option`s, with `Config` shaped as nested objects so it maps onto structured YAML.

## Import
```go
import "github.com/tuanlao/pulse/pkg/grpc/client"
```

## Configuration
**Top-level** (`mapstructure` tags):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `target` | `string` | `""` (**required**) | Dial target, e.g. `dns:///svc:9090` or `127.0.0.1:9090`. |
| `user_agent` | `string` | `"pulse-grpc-client"` | Sent on every call. |
| `max_recv_msg_size` | `int` | `4194304` | Max received message size (bytes). |
| `max_send_msg_size` | `int` | `4194304` | Max sent message size (bytes). |

**`timeouts.*`** (`TimeoutsConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `timeouts.request` | `duration` | `30s` | Per-call deadline, applied **only when the caller's ctx has no deadline**. Streaming RPCs are not auto-bounded. |

**`keepalive.*`** (`KeepaliveConfig`, maps to `keepalive.ClientParameters`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `keepalive.time` | `duration` | `30s` | Ping the server after this much idle time. |
| `keepalive.timeout` | `duration` | `20s` | Wait for a ping ack before treating the conn as dead. |
| `keepalive.permit_without_stream` | `bool` | `true` | Allow pings with no active RPCs. |

**`retry.*`** (`RetryConfig`, compiled to a gRPC service-config retry policy):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `retry.enabled` | `bool` | `true` | Toggles retries. |
| `retry.max_attempts` | `int` | `3` | Total attempts incl. the first (>= 2 to be meaningful; gRPC clamps the channel max). |
| `retry.initial_backoff` | `duration` | `100ms` | First retry backoff. |
| `retry.max_backoff` | `duration` | `2s` | Caps the exponential backoff. |
| `retry.backoff_multiplier` | `float64` | `2.0` | Backoff growth per attempt. |
| `retry.retryable_status_codes` | `[]string` | `["UNAVAILABLE"]` | gRPC codes that trigger a retry. |
| `retry.raw_service_config` | `string` | `""` | Raw gRPC service-config JSON; when set it overrides the generated policy verbatim (seam for hedging/throttling/per-method config). |

**`trace.*`** (`TraceConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `trace.propagate` | `bool` | `true` | Emit the custom trace-id metadata header on the tracing-off path. |
| `trace.trace_id_header` | `string` | `"x-trace-id"` | Custom trace-id metadata key (gRPC lowercases keys). |

**`metrics.*`** (`MetricsConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics.enabled` | `bool` | `true` | Toggles outbound metrics. |
| `metrics.namespace` | `string` | `"pulse"` | Prometheus namespace. |
| `metrics.subsystem` | `string` | `"grpc_client"` | Prometheus subsystem. |
| `metrics.buckets` | `[]float64` | `0.005 … 10` | Duration histogram buckets (seconds). |

**`tls.*`** (`TLSConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `tls.enabled` | `bool` | `false` | Dial over TLS. When false, insecure credentials are used. |
| `tls.ca_file` | `string` | `""` | Custom CA (PEM). **Optional** — empty uses the system root pool. |
| `tls.server_name_override` | `string` | `""` | Override the verified server name (SNI / testing). |
| `tls.insecure_skip_verify` | `bool` | `false` | Skip server cert verification (seam; avoid in prod). |
| `tls.cert_file` / `tls.key_file` | `string` | `""` | Client certificate for mTLS (seam). |

## Usage
```go
cm, _ := client.NewMetrics(cfg.Metrics, sharedRegistry) // optional, shares /metrics
c, err := client.New(cfg, client.Deps{
    Logger:         logger, // *log.Logger, optional
    Metrics:        cm,     // *client.ClientMetrics, optional
    TracerProvider: tp,     // optional; no-op/nil => manual x-trace-id injection
}, client.WithRequestTimeout(10*time.Second))
if err != nil { return err }

mgr.Register(c)                       // Stop closes the connection
stub := pb.NewMyServiceClient(c.Conn())
resp, err := stub.DoThing(ctx, req)   // per-call timeout applies when ctx has no deadline
```

## API / Options / Deps
- `New(cfg Config, deps Deps, opts ...Option) (*Client, error)`.
- `lifecycle.Component`: `Name() // "grpc-client"`, `Start(ctx)` (**no-op** — lazy connect), `Stop(ctx)` (closes the conn). `CheckReady(ctx)` (fails only on a `Shutdown` connection).
- `(*Client).Conn() *grpc.ClientConn` (build typed stubs), `(*Client).Config() Config`.
- `NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*ClientMetrics, error)`; `(*ClientMetrics).Registry()`.
- **Deps**: `Logger` (nil disables logging), `Metrics` (nil disables metrics), `TracerProvider` (nil/no-op → manual id injection), `Propagator` (default `propagation.TraceContext{}`).
- **Options**: `WithTarget`, `WithRequestTimeout`, `WithRetry`, `WithServiceConfig`, `WithTLS`, `WithTraceIDHeader`, `WithUserAgent`.

## Notes
- **`grpc.NewClient`, not `grpc.Dial`** (the latter is deprecated). It defaults to the DNS resolver and is **lazy**: the connection opens on the first RPC, with background reconnection handled by gRPC. `Start()` is therefore a no-op — it does not eager-connect (which would only slow startup, raise spurious warnings and duplicate gRPC's reconnect logic).
- **Tracing uses the otelgrpc `StatsHandler`** (`otelgrpc.NewClientHandler`). With a real `TracerProvider` the client emits the W3C `traceparent` (carrying the trace id) and does **not** add the custom header. With no real tracer, the client generates a trace id (`ensureIDs`) and injects **only** the custom `x-trace-id` header — it does **not** synthesize a `traceparent`, since with no upstream span that would falsely tell the server a traced span exists. (A documented, deliberate divergence from the HTTP client, which always emits both.)
- **Per-call timeout comes from the context.** gRPC has no transport-level per-call timeout; `timeouts.request` is applied by an interceptor only when the caller's ctx has no deadline. Streaming RPCs are not auto-bounded — bound them yourself.
- **Retry** is the gRPC built-in service-config retry policy (`WithDefaultServiceConfig`). Retries are not separately counted; the duration histogram captures the whole logical call (all retries collapsed). Set `raw_service_config` to take full control.
- **`CheckReady` only fails on `Shutdown`.** Idle/Connecting/TransientFailure are all usable (RPCs connect or retry), so they don't fail readiness — that would needlessly mark the pod NotReady while gRPC reconnects.
- Client RED metrics are labelled `target`/`method`/`grpc_type`/`code` (target is the configured dial target, bounded). A caller-side cancellation/deadline is labelled `canceled` and logged as info, keeping client cancellations out of upstream-error signals.
- `Registry()` is for **exposing** the metrics (scrape), not runtime `Gather`.
- The `true`-by-default toggles (`retry.enabled`, `trace.propagate`) are tri-state (`*bool`) internally: a key omitted from YAML is backfilled to its default, so the documented default holds even for a partial config. Set the key explicitly to `false` to turn it off.
