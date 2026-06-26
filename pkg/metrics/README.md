# `pkg/metrics`

Prometheus instrumentation for HTTP services using the RED method (Rate, Errors, Duration). It uses the native `prometheus/client_golang` library with a package-owned registry — never the global default registry and never the OTel→Prometheus bridge — so collectors, buckets and labels stay fully under control and tests remain hermetic. Provides a gin middleware for request accounting and an `http.Handler` for the `/metrics` scrape endpoint.

## Import
```go
import "github.com/tuanlao/pulse/pkg/metrics"
```

## Configuration
| YAML key (`mapstructure` tag) | Type | Default (from `DefaultConfig`) | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Toggles metrics collection and the `/metrics` endpoint. |
| `path` | `string` | `"/metrics"` | Scrape endpoint path. |
| `namespace` | `string` | `"pulse"` | Prometheus metric namespace. |
| `subsystem` | `string` | `"http"` | Prometheus metric subsystem. |
| `duration_buckets` | `[]float64` | `[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]` | Request-duration histogram buckets (seconds). |

## Usage
```go
red, err := metrics.New(metrics.DefaultConfig(), metrics.WithNamespace("orders"))
if err != nil {
    panic(err)
}

r := gin.New()
// Record RED metrics; skip infra/probe routes to avoid noise.
r.Use(red.Middleware("/metrics", "/healthz", "/readyz"))
r.GET("/metrics", gin.WrapH(red.Handler()))
```

## API / Options
- `New(cfg Config, opts ...Option) (*RED, error)` — build the RED instrument set on a fresh, package-owned registry (also registers the Go runtime and process collectors).
- `(*RED).Middleware(skip ...string) gin.HandlerFunc` — gin middleware recording duration, totals and in-flight gauge.
- `(*RED).Handler() http.Handler` — serves the registry in Prometheus exposition format.
- `(*RED).Registry() *prometheus.Registry` / `(*RED).Config() Config`.
- Options: `WithPath`, `WithNamespace`, `WithBuckets`.

Metrics emitted (under `<namespace>_<subsystem>_`): `request_duration_seconds` (histogram), `requests_total` (counter), `requests_in_flight` (gauge). Labels: `method`, `route`, `status`.

## Notes
- The `route` label uses the gin route **pattern** via `c.FullPath()` (e.g. `/users/:id`), never the raw path, to keep cardinality bounded. Unmatched routes (404) collapse to a single `"unmatched"` series.
- Paths passed to `Middleware(skip ...)` are excluded entirely (e.g. `/metrics`, `/healthz`, `/readyz`).
- Observation happens in a `defer`, so requests are still counted when an inner handler panics: the status is forced to `500`, the metric is recorded, then the panic is re-raised for the outer recovery middleware.
- The registry is package-owned (`prometheus.NewRegistry()`), so multiple instances don't collide and the global default registry is untouched.
