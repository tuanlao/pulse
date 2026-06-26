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
  (`pkg/config`). There is no god aggregator config: a service owns its own config
  struct and composes the component `Config`s it needs.
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
- **Redis** — `rueidis` client with full config and, as its headline feature,
  **client-side caching** including the cheap **broadcast (BCAST) prefix** mode,
  plus standalone/cluster/Sentinel/TLS, per-command spans and Prometheus metrics.

## Install

```sh
go get github.com/tuanlao/pulse@latest
```

## Quickstart

Define your service's own config (embedding the component `Config`s you need),
load it with `pkg/config`, and compose a service (see
[`examples/service`](examples/service/README.md) for the full version):

```go
// The service owns its config — there is no unified app.Config.
type Config struct {
    Server  server.Config  `mapstructure:"server"`
    Log     log.Config     `mapstructure:"log"`
    Tracing tracing.Config `mapstructure:"tracing"`
    Metrics metrics.Config `mapstructure:"metrics"`
    // ...component configs you need + your own fields
}

cfg := Config{Server: server.DefaultConfig(), Log: log.DefaultConfig(), Tracing: tracing.DefaultConfig(), Metrics: metrics.DefaultConfig()}
_ = config.Load(&cfg, config.DefaultOptions())       // defaults < config.yaml

logger, _ := log.New(cfg.Log)
defer logger.Sync()                                  // flushed last

tracer, _ := tracing.New(ctx, cfg.Tracing)           // OTLP; lifecycle.Component
red, _ := metrics.New(cfg.Metrics)                   // Prometheus RED

srv, _ := server.New(cfg.Server, server.Deps{Logger: logger, Metrics: red, TracerProvider: tracer.Provider()})
srv.Engine().GET("/hello", func(c *gin.Context) { c.JSON(200, gin.H{"ok": true}) })

mgr := lifecycle.New(lifecycle.DefaultConfig(), logger.LifecycleAdapter())
mgr.Register(tracer)                                 // tracing first ...
mgr.Register(srv)                                    // ... server last (stops first)
_ = mgr.Run(ctx)                                     // blocks until SIGINT/SIGTERM, then ordered shutdown
```

Use `pkg/env` for the deployment `Env` enum (LOCAL/DEV/UAT/STG/PROD) and to
derive the gin mode (`cfg.Server.Mode = cfg.Env.GinMode()`).

## Packages

| Package | Description |
|---|---|
| [`pkg/config`](pkg/config/README.md) | Generic viper loader: `Load[T]`, precedence, nested-object YAML |
| [`pkg/env`](pkg/env/README.md) | Deployment `Env` enum (LOCAL/DEV/UAT/STG/PROD) + gin-mode helper |
| [`pkg/lifecycle`](pkg/lifecycle/README.md) | `Component` interface + `Manager` (signals, ordered/reverse shutdown) |
| [`pkg/log`](pkg/log/README.md) | zap logger, `[time][LEVEL]` format, context-aware trace/span fields |
| [`pkg/tracing`](pkg/tracing/README.md) | OpenTelemetry TracerProvider (OTLP grpc/http), trace-id helpers |
| [`pkg/metrics`](pkg/metrics/README.md) | Prometheus RED metrics, package-owned registry |
| [`pkg/contextx`](pkg/contextx/README.md) | Leaf context carrier (request-scoped logger) that breaks import cycles |
| [`pkg/http/server`](pkg/http/server/README.md) | gin HTTP server, timeouts, health/readiness, lifecycle |
| [`pkg/http/server/middleware`](pkg/http/server/middleware/README.md) | recovery, request-id, context-logger, RED, CORS, body-limit |
| [`pkg/http/client`](pkg/http/client/README.md) | Outbound client: pool, retries, JSON, always-on trace propagation |
| [`pkg/cron`](pkg/cron/README.md) | gocron scheduler: tracing, metrics, recovery, config-declared jobs, redis distributed lock |
| [`pkg/redis`](pkg/redis/README.md) | rueidis client: client-side caching (incl. BCAST prefixes), cluster/Sentinel/TLS, spans + metrics |
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

Implemented: config, env, lifecycle, log, tracing, metrics, http server + client,
cron, redis, swagger, version. Planned (future phases): gRPC, database, kafka —
each will slot in as a sibling package implementing `lifecycle.Component`.
