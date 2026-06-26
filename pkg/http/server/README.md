# `pkg/http/server`

A configurable gin-based HTTP server that plugs into the pulse lifecycle. It implements `lifecycle.Component`, builds the underlying `http.Server` manually with safe timeouts (including a `ReadHeaderTimeout` Slowloris guard), wires the standard middleware chain (recovery, OTel tracing, request logger, RED metrics, CORS, body limit) and exposes separate `/healthz` (liveness) and `/readyz` (readiness, backed by a `ReadinessRegistry`) endpoints.

## Import
```go
import "github.com/tuanlao/pulse/pkg/http/server"
```

## Configuration
| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `port` | `int` | `8080` | TCP port to listen on (binds all interfaces, `:<port>`). |
| `mode` | `string` | `"release"` | gin mode: `release`, `debug` or `test`. |
| `read_timeout` | `duration` | `15s` | Max duration to read the entire request. |
| `write_timeout` | `duration` | `15s` | Max duration before timing out writes. |
| `idle_timeout` | `duration` | `60s` | Max keep-alive idle time. |
| `read_header_timeout` | `duration` | `5s` | Caps header reads (Slowloris guard). |
| `shutdown_drain` | `duration` | `25s` | Bounds in-flight draining on `Stop`; should be `<=` the lifecycle ShutdownTimeout. |
| `max_body_bytes` | `int64` | `1048576` (1 MiB) | Caps request body size; non-positive disables. |
| `healthz_path` | `string` | `"/healthz"` | Liveness endpoint. |
| `readyz_path` | `string` | `"/readyz"` | Readiness endpoint. |
| `readiness_check_timeout` | `duration` | `2s` | Bounds each readiness check. |
| `cors` | `middleware.CORSConfig` | disabled | Cross-origin handling (see `pkg/http/server/middleware`). |
| `tls.enabled` / `tls.cert_file` / `tls.key_file` | `TLSConfig` | disabled | Seam reserved for future TLS support. |
| `gzip.enabled` / `gzip.level` | `GzipConfig` | disabled | Seam reserved for future response compression. |

## Usage
```go
cfg := server.DefaultConfig()
srv, err := server.New(cfg, server.Deps{
    Logger:         logger,        // *log.Logger
    Metrics:        red,           // *metrics.RED, optional
    TracerProvider: tp,            // trace.TracerProvider, optional
    ServiceName:    "my-service",  // labels otelgin spans
}, server.WithPort(9090))
if err != nil {
    return err
}

// Register application routes on the gin engine.
srv.Engine().GET("/users/:id", handleGetUser)

// Register readiness checks for dependencies.
srv.Readiness().Register("postgres", func(ctx context.Context) error { return db.PingContext(ctx) })

// Lifecycle: register the server LAST so it is the first component stopped.
lifecycleManager.Register(srv) // srv implements lifecycle.Component
```

## API / Options / Deps
- `New(cfg Config, deps Deps, opts ...Option) (*Server, error)` — constructs and wires everything.
- `(*Server).Engine() *gin.Engine` — register application routes.
- `(*Server).Readiness() *ReadinessRegistry` — register dependency readiness checks.
- `(*Server).Config() Config` — resolved configuration.
- `(*Server).Name() string` / `Start(ctx) error` / `Stop(ctx) error` — `lifecycle.Component`.
- `ReadinessRegistry`: `NewReadinessRegistry(perCheckTimeout)`, `Register(name, ReadinessCheck)`, `Evaluate(ctx) (map[string]Result, bool)`; `ReadinessCheck func(ctx) error`; `Result{Status, Error}`.
- **Deps**: `Logger *log.Logger` (required; nil falls back to `log.Nop()`), `Metrics *metrics.RED` (nil disables RED + scrape route), `TracerProvider trace.TracerProvider` (nil disables otelgin), `ServiceName string` (default `"pulse-http"`), `OnServeError func(error)` (optional; called when the background `Serve` goroutine exits with a non-`ErrServerClosed` error — wire it to cancel the lifecycle context so a fatal serve failure shuts the process down instead of running on with a dead listener).
- **Options**: `WithPort(port)`, `WithMode(mode)`, `WithCORS(middleware.CORSConfig)`.
- `DefaultConfig() Config` returns the table above.

## Notes
- The underlying `http.Server` is built manually with `ReadTimeout`, `WriteTimeout`, `IdleTimeout` and `ReadHeaderTimeout` (the Slowloris guard) — it does **not** use `engine.Run()`.
- `Start` binds the listener synchronously (so bind errors surface to the lifecycle manager) and serves in a background goroutine; it does not block. A later fatal `Serve` error is logged and, if `Deps.OnServeError` is set, reported so the process can shut down.
- `/readyz` evaluates each check under `readiness_check_timeout` and bounds it even if a check ignores its context, so a misbehaving check cannot hang the endpoint.
- `Stop` drains in-flight requests within the smaller of the context deadline and `ShutdownDrain`.
- Two separate endpoints: `/healthz` (liveness, returns version) and `/readyz` (readiness, evaluates the `ReadinessRegistry` concurrently and returns `503` when any check fails).
- When `Deps.Metrics` is set, the Prometheus scrape endpoint is mounted at its configured path and the health + metrics paths are excluded from RED metrics.
- `Name()` returns `"http"`. Register the server **LAST** in the lifecycle so it stops first.
