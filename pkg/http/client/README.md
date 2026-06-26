# `pkg/http/client`

A configurable outbound HTTP client for calling other services. It provides connection pooling, per-call context handling (creating one if absent), guaranteed trace-id propagation (W3C `traceparent` + a customizable `X-Trace-Id` header, generating ids when the context has none), retries with backoff, JSON helpers, client-side RED metrics and outbound request logging. Like every pulse package it exposes `Config` + `DefaultConfig()` + functional `Option`s, with `Config` shaped as nested objects (`timeouts`/`pool`/`retry`/`trace`/`metrics`) so it maps onto structured YAML.

## Import
```go
import "github.com/tuanlao/pulse/pkg/http/client"
```

## Configuration
**Top-level** (`mapstructure` tags):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `base_url` | `string` | `""` | Base URL. Relative request paths are **appended** onto the base path (e.g. base `.../v1` + `/users` → `.../v1/users`); they do not replace it. |
| `user_agent` | `string` | `"pulse-client"` | Sent on every request. |

**`timeouts.*`** (`TimeoutsConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `timeouts.request` | `duration` | `30s` | Bounds the whole call (all retries) via context. |
| `timeouts.dial` | `duration` | `5s` | TCP dial timeout. |
| `timeouts.tls_handshake` | `duration` | `5s` | TLS handshake timeout. |
| `timeouts.response_header` | `duration` | `10s` | Wait for response headers per attempt. |
| `timeouts.expect_continue` | `duration` | `1s` | 100-continue timeout. |
| `timeouts.keep_alive` | `duration` | `30s` | Dialer keep-alive period. |

**`pool.*`** (`PoolConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `pool.max_idle_conns` | `int` | `100` | Total idle-conn cap. |
| `pool.max_idle_conns_per_host` | `int` | `10` | Idle conns per host. |
| `pool.max_conns_per_host` | `int` | `0` | Total conns per host (0 = unlimited). |
| `pool.idle_conn_timeout` | `duration` | `90s` | How long an idle conn is kept. |
| `pool.disable_keep_alives` | `bool` | `false` | Disables connection reuse. |

**`retry.*`** (`RetryConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `retry.enabled` | `bool` | `true` | Toggles retries. |
| `retry.max_attempts` | `int` | `3` | Total attempts including the first. |
| `retry.base_backoff` | `duration` | `100ms` | Initial backoff. |
| `retry.max_backoff` | `duration` | `2s` | Caps the exponential backoff. |
| `retry.jitter` | `bool` | `true` | Applies full jitter to backoff. |
| `retry.methods` | `[]string` | `GET,HEAD,PUT,DELETE,OPTIONS` | Retryable (idempotent) methods. |
| `retry.retry_statuses` | `[]int` | `502,503,504` | Retryable status codes. |
| `retry.respect_retry_after` | `bool` | `true` | Honors the `Retry-After` header. |

**`trace.*`** (`TraceConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `trace.inject_traceparent` | `bool` | `true` | Emits the W3C `traceparent` header. |
| `trace.propagate` | `bool` | `true` | Emits the custom trace-id header. |
| `trace.trace_id_header` | `string` | `"X-Trace-Id"` | Custom trace-id header name. |

**`metrics.*`** (`MetricsConfig`):
| YAML key | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics.enabled` | `bool` | `true` | Toggles outbound metrics. |
| `metrics.namespace` | `string` | `"pulse"` | Prometheus namespace. |
| `metrics.subsystem` | `string` | `"http_client"` | Prometheus subsystem. |
| `metrics.buckets` | `[]float64` | `0.005 … 10` | Duration histogram buckets (seconds). |

## Usage
```go
cfg := client.DefaultConfig()
cfg.BaseURL = "https://api.example.com"

cm, _ := client.NewMetrics(cfg.Metrics, sharedRegistry) // optional, shares /metrics
c, err := client.New(cfg, client.Deps{
    Logger:         logger,        // *log.Logger, optional
    Metrics:        cm,            // *client.ClientMetrics, optional
    TracerProvider: tp,            // optional; no-op provider => manual id injection
}, client.WithRequestTimeout(10*time.Second))
if err != nil {
    return err
}

// JSON helpers.
var user User
if err := c.GetJSON(ctx, "/users/42", &user); err != nil { /* may be *client.HTTPError */ }
if err := c.PostJSON(ctx, "/users", newUser, &created); err != nil { /* ... */ }

// Lower-level call (caller closes resp.Body).
resp, err := c.Do(ctx, &client.Request{
    Method:   http.MethodPost,
    Path:     "/things",
    JSONBody: payload, // retry-safe (sets GetBody)
}, )
```

## API / Options / Deps
- `New(cfg Config, deps Deps, opts ...Option) (*Client, error)`.
- `(*Client).Do(ctx, *Request) (*http.Response, error)` — applies per-call timeout, trace id, URL resolution, default headers; caller closes the body.
- JSON helpers: `GetJSON`, `PostJSON`, `PutJSON`, `DeleteJSON(ctx, path, [in,] out, opts...) error`; non-2xx returns `*HTTPError{Method, URL, StatusCode, Status, Snippet}`.
- `(*Client).HTTPClient() *http.Client`, `(*Client).Config() Config`.
- `Request{Method, Path, Body, JSONBody, Header, Query}`; **RequestOption**s: `WithHeader(k,v)`, `WithQuery(k,v)`, `WithNoRetry()`.
- `NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*ClientMetrics, error)`; `(*ClientMetrics).Registry()`.
- **Deps**: `Logger *log.Logger` (nil disables logging), `Metrics *ClientMetrics` (nil disables metrics), `TracerProvider trace.TracerProvider` (nil/no-op => manual id injection), `Propagator propagation.TextMapPropagator` (default `propagation.TraceContext{}`).
- **Options**: `WithBaseURL`, `WithPoolSize(maxIdlePerHost, maxPerHost)`, `WithRequestTimeout`, `WithRetry`, `WithTraceIDHeader`.
- `DefaultConfig() Config` returns the tables above.

## Notes
- The client **always emits a trace id**: it injects the W3C `traceparent` plus the customizable `X-Trace-Id` header (set from the *trace* id, stable across parent/child), generating a span context when the context has none (`ensureIDs` at the top of every call).
- A real (exporting) `TracerProvider` routes through `otelhttp` (which owns the span + `traceparent`) wrapped by `headerRT` for the custom headers; a nil/no-op provider uses `manualTraceRT` to inject the headers directly — so the emitted trace id is identical either way.
- Retry replays the body via `GetBody` (set automatically for `JSONBody`, `*bytes.Reader`, `*strings.Reader`); a body without `GetBody` falls back to a single attempt. Backoff is exponential + capped + optional full jitter, honoring `Retry-After`.
- Client RED metrics are labelled `host`/`method`/`status` (host only, bounded cardinality) and recorded once per logical call; a separate `retries_total` is labelled `host`/`method`.
- There is **no `http.Client.Timeout`** — the per-call deadline comes from `timeouts.request` applied to the context, so it bounds all retries together.
- The `true`-by-default toggles (`retry.enabled`, `retry.respect_retry_after`, `trace.inject_traceparent`, `trace.propagate`) are tri-state (`*bool`) internally: a key omitted from YAML is treated as "unset" and backfilled to its default by `applyDefaults`, so the documented default holds even for a partial config (e.g. an `app.HttpClients` map entry). Set the key explicitly to `false` to turn the behavior off.
- A caller-side cancellation or deadline (`ctx` done) is recorded distinctly: the RED `status` label is `canceled` (not `error`) and the log line is an info `outbound request canceled`, so client cancellations don't pollute upstream-error signals.
