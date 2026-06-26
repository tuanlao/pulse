# pulse

A common Go library for building microservices. Pulse provides small,
**composable** building blocks — HTTP server & client, config, logging, tracing,
metrics, cron scheduling, Swagger — that each initialise independently and
register into a single lifecycle manager. There is no god `pulse.New()`: a
service wires only what it needs.

Module: `github.com/tuanlao/pulse` · Go 1.26+

## Highlights

- **Composable lifecycle** — every long-lived component implements
  `lifecycle.Component` (`Name/Start/Stop`); the manager starts in registration
  order and stops in reverse, with a shutdown timeout and aggregated errors.
- **Everything configurable, with defaults** — every package exposes
  `Config` + `DefaultConfig()` + functional `Option`s. Config is **nested-object**
  shaped (maps onto structured YAML, no env vars or flags), loadable via viper
  (`pkg/config`) or the unified `app.Config`.
- **Observability built in** — zap logging with `[time][LEVEL]` console format,
  OpenTelemetry tracing (OTLP, or ids-only without a collector), and Prometheus
  RED metrics. The OTel trace id and span id flow through context into logs,
  traces, and HTTP headers.
- **Production HTTP** — gin server with safe `http.Server` timeouts (incl.
  `ReadHeaderTimeout` Slowloris guard), separate `/healthz` + `/readyz`, panic
  recovery, CORS, body limits; and an outbound client with pooling, retries,
  JSON helpers, and **guaranteed trace-id propagation**.
- **Cron** — `go-co-op/gocron/v2` scheduler with per-job tracing, logging, panic
  recovery, metrics, timeout, singleton mode, jobs declarable in config, and an
  opt-in **redis distributed lock** so each run is taken by one pod — spreading
  load across pods in a multi-pod deployment.

## Install

```sh
go get github.com/tuanlao/pulse@latest
```

## Quickstart

Load the unified config and compose a service (see
[`examples/service`](examples/service/README.md) for the full version):

```go
cfg := app.DefaultConfig()
cfg.ServiceName = "my-service"
_ = app.Load(&cfg, config.DefaultOptions())          // defaults < config.yaml

logger, _ := log.New(cfg.Log)
defer logger.Sync()                                  // flushed last

tracer, _ := tracing.New(ctx, cfg.Tracing)           // OTLP; lifecycle.Component
red, _ := metrics.New(cfg.Metrics)                   // Prometheus RED

srv, _ := server.New(cfg.Server, server.Deps{Logger: logger, Metrics: red, TracerProvider: tracer.Provider()})
srv.Engine().GET("/hello", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })

mgr := lifecycle.New(cfg.Lifecycle, logger.LifecycleAdapter())
mgr.Register(tracer)                                 // tracing first ...
mgr.Register(srv)                                    // ... server last (stops first)
_ = mgr.Run(ctx)                                     // blocks until SIGINT/SIGTERM, then ordered shutdown
```

## Packages

| Package | Description |
|---|---|
| [`pkg/app`](pkg/app/README.md) | Unified `AppConfig` (Env + every component config + `HttpClients`) and loader |
| [`pkg/config`](pkg/config/README.md) | Generic viper loader: `Load[T]`, precedence, nested-object YAML |
| [`pkg/lifecycle`](pkg/lifecycle/README.md) | `Component` interface + `Manager` (signals, ordered/reverse shutdown) |
| [`pkg/log`](pkg/log/README.md) | zap logger, `[time][LEVEL]` format, context-aware trace/span fields |
| [`pkg/tracing`](pkg/tracing/README.md) | OpenTelemetry TracerProvider (OTLP grpc/http), trace-id helpers |
| [`pkg/metrics`](pkg/metrics/README.md) | Prometheus RED metrics, package-owned registry |
| [`pkg/contextx`](pkg/contextx/README.md) | Leaf context carrier (request-scoped logger) that breaks import cycles |
| [`pkg/http/server`](pkg/http/server/README.md) | gin HTTP server, timeouts, health/readiness, lifecycle |
| [`pkg/http/server/middleware`](pkg/http/server/middleware/README.md) | recovery, request-id, context-logger, RED, CORS, body-limit |
| [`pkg/http/client`](pkg/http/client/README.md) | Outbound client: pool, retries, JSON, always-on trace propagation |
| [`pkg/cron`](pkg/cron/README.md) | gocron scheduler: tracing, metrics, recovery, config-declared jobs, redis distributed lock |
| [`pkg/swagger`](pkg/swagger/README.md) | Swagger UI mount (disabled by default) |
| [`pkg/version`](pkg/version/README.md) | Build metadata injected via ldflags |
| [`examples/service`](examples/service/README.md) | Canonical composition root |

## Conventions

Every package follows the same shape — see [`CLAUDE.md`](CLAUDE.md) for details:

1. **Configurable** — `Config` (with `mapstructure` tags) + `DefaultConfig()` + `Option`s.
2. **Sensible defaults** — `DefaultConfig()` is the source of truth; sources only override.
3. **Tested** — table-driven, run with `-race`.
4. **Composable** — independent components registered into `lifecycle.Manager`; no all-or-nothing.
5. **Documented** — every package ships a `README.md`.

## Development

```sh
go build ./...
go test -race -cover ./pkg/...
go vet ./...
golangci-lint run        # if installed
```

## Status

Implemented: config, lifecycle, log, tracing, metrics, http server + client,
cron, swagger, app, version. Planned (future phases): gRPC, database, redis,
kafka — each will slot in as a sibling package implementing `lifecycle.Component`.
