# `pkg/http/middleware`

The gin middleware that pulse's HTTP server (`pkg/http/server`) wires together: panic recovery, request-scoped logging, CORS and request body-size limiting. RED metrics live in `pkg/metrics` and OTel span instrumentation comes from `otelgin`; both are also wired by the server. This package contains no HTTP server itself — it only exports the handler factories the server composes.

## Import
```go
import "github.com/tuanlao/pulse/pkg/http/server/middleware"
```

## Configuration
**CORSConfig** (`mapstructure` tags; embedded as `cors` in the server config):
| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `false` | Toggles CORS handling (pass-through when off). |
| `allow_origins` | `[]string` | `["*"]` | Allowed origins; `"*"` allows any. |
| `allow_methods` | `[]string` | `GET,POST,PUT,PATCH,DELETE,OPTIONS` | Allowed methods. |
| `allow_headers` | `[]string` | `Origin,Content-Type,Accept,Authorization` | Allowed request headers. |
| `expose_headers` | `[]string` | `nil` | Response headers exposed to the browser. |
| `allow_credentials` | `bool` | `false` | Sets `Access-Control-Allow-Credentials`. |
| `max_age_seconds` | `int` | `600` | `Access-Control-Max-Age` for preflight caching. |

## Usage
This package is wired by `pkg/http/server`, which installs the chain from outermost to innermost:
```go
e := gin.New()
e.Use(middleware.Recovery(logger))                                       // 1. recovery (outermost)
e.Use(otelgin.Middleware(serviceName, otelgin.WithTracerProvider(tp)))   // 2. otelgin (if provider set)
e.Use(middleware.ContextLogger(logger))                                  // 3. context logger
e.Use(red.Middleware(skipPaths...))                                      // 4. RED metrics (if metrics set)
e.Use(middleware.CORS(corsCfg))                                          // 5. CORS
e.Use(middleware.BodyLimit(maxBodyBytes))                                // 6. body limit
```

## API / Options
- `Recovery(base *log.Logger) gin.HandlerFunc` — outermost; recovers panics, logs with the request-scoped logger, responds `500` if nothing written yet.
- `ContextLogger(base *log.Logger) gin.HandlerFunc` — builds a request-scoped logger carrying (when tracing is active) the trace/span ids, stored via `log.IntoContext`.
- `CORS(cfg CORSConfig) gin.HandlerFunc` — pass-through when `cfg.Enabled` is false.
- `BodyLimit(maxBytes int64) gin.HandlerFunc` — wraps the body in `http.MaxBytesReader`; non-positive disables.
- `DefaultCORSConfig() CORSConfig`.

## Notes
- The server installs the chain in the order recovery → otelgin → contextLogger → RED → cors → bodylimit. Recovery is outermost so it catches panics from everything inside; `ContextLogger` runs after `otelgin` so the trace/span ids are available.
- RED metrics (from `pkg/metrics`) records a `500` on panic because `Recovery` re-panics/aborts after the metrics middleware has wrapped the handler, so the RED layer observes the `500` status.
- When credentials are allowed, a `"*"` origin is reflected back as the concrete origin (per the CORS spec `"*"` is invalid with credentials) and `Vary: Origin` is set.
- `OPTIONS` preflight requests are answered with `204 No Content` plus the allow-methods/allow-headers/max-age headers.
