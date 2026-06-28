# `pkg/grpc`

The gRPC subsystem for pulse — a configurable **server** and outbound **client** that mirror `pkg/http`'s conventions and observability (OTel tracing, `pkg/log`, package-owned Prometheus metrics) and plug into the pulse `lifecycle.Manager`.

Like `pkg/http`, there is **no facade**: `server`, `server/interceptor` and `client` are independent packages. A service composes the ones it needs in its own `appConfig` (see `examples/service`).

## Subpackages
| Package | Purpose |
| --- | --- |
| [`server`](server/) | gRPC server (`lifecycle.Component`): `grpc.NewServer` with safe keepalive/message-size, the interceptor chain, otelgrpc tracing, the gRPC health service (default on) and optional reflection; graceful drain on shutdown. |
| [`server/interceptor`](server/interceptor/) | The server interceptors: panic **Recovery** (→ `codes.Internal`), request-scoped **ContextLogger** (trace/span ids), and **RED metrics** (`grpc_service`/`grpc_method`/`grpc_type`/`grpc_code`). |
| [`client`](client/) | Outbound gRPC client (`lifecycle.Component`) owning a `*grpc.ClientConn` built with the modern lazy `grpc.NewClient`: otelgrpc tracing (or manual `x-trace-id` correlation), per-call timeout, service-config retry, keepalive, TLS, client-side RED metrics + logging. |

## Quick start
```go
// Server: register your generated stub, then run it in the lifecycle manager.
gsrv, _ := server.New(cfg.Grpc, server.Deps{Logger: logger, Metrics: grpcMetrics, TracerProvider: tp, ServiceName: "my-svc"})
gsrv.Register(func(s *grpc.Server) { pb.RegisterMyServiceServer(s, impl) }) // BEFORE Start
mgr.Register(gsrv) // registered last → drains first

// Client: dial once, build a typed stub from the shared conn.
gc, _ := client.New(cfg.GrpcClient, client.Deps{Logger: logger, Metrics: clientMetrics, TracerProvider: tp})
mgr.Register(gc)
stub := pb.NewMyServiceClient(gc.Conn())
```

## Notes
- **Modern gRPC APIs:** the client uses `grpc.NewClient` (not the deprecated `grpc.Dial`) and tracing uses the otelgrpc **StatsHandler** (not the deprecated interceptors).
- **No protoc needed to test:** the integration tests run fully in-process over a real TCP listener using the built-in health service (unary + streaming) plus a tiny hand-written service built on the well-known `wrapperspb`/`emptypb` types — no external infra (`go test -tags=integration ./pkg/grpc/...`).
- See each subpackage README for the full Config tables and gotchas.
