# `pkg/grpc/server`

A configurable gRPC server that plugs into the pulse lifecycle. It implements `lifecycle.Component`, builds a `*grpc.Server` with safe message-size and keepalive settings, chains the standard interceptors (recovery, request logger, RED metrics) plus otelgrpc tracing via a **StatsHandler**, registers the gRPC health service (default on) and optional reflection, and gracefully drains in-flight RPCs on shutdown. Like every pulse package it exposes `Config` + `DefaultConfig()` + functional `Option`s.

## Import
```go
import "github.com/tuanlao/pulse/pkg/grpc/server"
```

## Configuration
**Top-level** (`mapstructure` tags):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `port` | `int` | `9090` | TCP port (binds all interfaces, `:<port>`). |
| `network` | `string` | `"tcp"` | Listen network. |
| `max_recv_msg_size` | `int` | `4194304` | Max received message size (bytes). |
| `max_send_msg_size` | `int` | `4194304` | Max sent message size (bytes). |
| `connection_timeout` | `duration` | `120s` | Bounds connection setup (handshake). |
| `shutdown_drain` | `duration` | `25s` | Bounds `GracefulStop` before a hard `Stop`; keep `<=` lifecycle `shutdown_timeout`. |
| `enable_reflection` | `bool` | `false` | Register the reflection service (opt-in; keep off in prod — it exposes the schema). |
| `enable_health` | `bool` | `true` | Register the standard gRPC health service. |

**`keepalive.*`** (`KeepaliveConfig`, maps to `keepalive.ServerParameters` + `EnforcementPolicy`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `keepalive.time` | `duration` | `2h` | Ping an idle connection after this long. |
| `keepalive.timeout` | `duration` | `20s` | Wait for a ping ack before closing. |
| `keepalive.max_connection_idle` | `duration` | `0` | Close after this much idle time (0 = infinite). |
| `keepalive.max_connection_age` | `duration` | `0` | Cap a connection's total lifetime (0 = infinite). |
| `keepalive.max_connection_age_grace` | `duration` | `0` | Grace period after `max_connection_age`. |
| `keepalive.enforcement_min_time` | `duration` | `5m` | Minimum client ping interval tolerated. |
| `keepalive.enforcement_permit_without_stream` | `bool` | `true` | Allow client pings with no active streams. |

**`metrics.*`** — see [`server/interceptor`](interceptor/) (`interceptor.MetricsConfig`, subsystem `grpc_server`).

**`tls.*`** (`TLSConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `tls.enabled` | `bool` | `false` | Serve over TLS. When false, the listener is plaintext. |
| `tls.cert_file` | `string` | `""` | Server certificate (PEM). |
| `tls.key_file` | `string` | `""` | Server private key (PEM). |
| `tls.client_ca_file` | `string` | `""` | When set, require + verify a client cert (mTLS). |

## Usage
```go
grpcMetrics, _ := interceptor.NewMetrics(cfg.Metrics, sharedRegistry) // optional, shares /metrics; nil when disabled
srv, err := server.New(cfg, server.Deps{
    Logger:         logger,            // *log.Logger, optional (nil => no-op)
    Metrics:        grpcMetrics,       // *interceptor.Metrics, optional (nil => no RED)
    TracerProvider: tp,                // optional; no-op/nil => no StatsHandler tracing
    ServiceName:    "my-svc",          // labels otelgrpc spans
    OnServeError:   func(error) { cancel() },
})
if err != nil { return err }

// Register service implementations BEFORE Start.
srv.Register(func(s *grpc.Server) { pb.RegisterMyServiceServer(s, impl) })

mgr.Register(srv) // register LAST so it stops FIRST (drains before dependencies)
```

## API / Options / Deps
- `New(cfg Config, deps Deps, opts ...Option) (*Server, error)`.
- `lifecycle.Component`: `Name() // "grpc"`, `Start(ctx)` (binds synchronously, serves in a goroutine), `Stop(ctx)` (graceful drain → hard stop on timeout). `CheckReady(ctx)` (ready once started).
- `(*Server).Register(fn func(*grpc.Server))` — apply registration **before** `Start` (panics if called after); `(*Server).Server() *grpc.Server` escape hatch; `(*Server).Health() *health.Server`; `(*Server).SetServingStatus(service string, serving bool)`; `(*Server).Config() Config`.
- **Deps**: `Logger` (nil → no-op), `Metrics` (nil → no RED), `TracerProvider` (nil/no-op → no tracing), `ServiceName` (default `"pulse-grpc"`), `OnServeError`.
- **Options**: `WithPort`, `WithReflection`, `WithHealth`, `WithMaxRecvMsgSize`, `WithMaxSendMsgSize`, `WithTLS`, `WithKeepalive`.

## Notes
- **Tracing uses the otelgrpc `StatsHandler`** (`otelgrpc.NewServerHandler`), the modern API — the old `otelgrpc.UnaryServerInterceptor` is deprecated. The StatsHandler brackets the whole RPC, so trace/span ids are on the context before the `ContextLogger` interceptor runs.
- **Interceptor order** (outermost → innermost): Recovery → ContextLogger → Metrics. Recovery catches panics from inner layers; metrics records under a defer so a panic is still accounted for as `Internal`, then re-raised for Recovery to convert + log.
- **Health is the readiness surface.** `New` sets overall `NOT_SERVING`, `Start` flips it to `SERVING`, `Stop` sets `NOT_SERVING` + `Shutdown()` (so in-flight `Watch` streams observe it). Gate a service `SERVING` from your own dependency checks with `SetServingStatus`.
- **Graceful drain:** `Stop` runs `GracefulStop` raced against `shutdown_drain`; on timeout it falls back to a hard `Stop` that cancels in-flight RPCs.
- **`enable_health` is a default-true bool**: a literal `false` from a partial config is only honored when the struct originates from `DefaultConfig()` (which the service's `defaultAppConfig` does). Same caveat as the HTTP server's `tls.enabled`.
