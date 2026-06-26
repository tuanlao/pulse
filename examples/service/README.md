# Example service

This is the canonical composition root for pulse: a small service that wires the
components together using the unified `app.Config`. It demonstrates loading
`app.Config` (env + every component config) from `config.yaml`, building the
logger, tracing and metrics, standing up the HTTP server, and creating an
outbound HTTP client pointed back at itself (so the trace id propagates from the
inbound request through the client into headers). It also schedules a cron
heartbeat job — declared in
`config.yaml` (`cron.jobs.heartbeat`) and bound to its handler in code via
`cronSched.Register("heartbeat", ...)` — and registers everything
with the lifecycle manager for ordered shutdown (tracing first, server last, so
the server drains before tracing flushes; cron stops in between). Server, client
and cron metrics share one Prometheus registry, so a single `/metrics` endpoint
exposes all of them.

## Run

```sh
cd examples/service && go run .
```

Run from this directory so `.` is on the config search path (it loads
`config.yaml`). Config precedence is defaults < `config.yaml`.

The server listens on `:8080` by default (set `server.port` in `config.yaml` to
change it). Endpoints:

| Method & path | Description |
| --- | --- |
| `GET /healthz` | Liveness probe. |
| `GET /readyz` | Readiness probe (runs registered checks). |
| `GET /metrics` | Prometheus metrics (server RED + outbound client + cron). |
| `GET /hello/:name` | Returns `hello <name>` with a timestamp. |
| `GET /call/:name` | Calls this service's own `/hello/:name` via the outbound client, propagating the trace id. |

Try it:

```sh
curl localhost:8080/hello/world
curl localhost:8080/call/world
curl localhost:8080/metrics
```

## Tracing

Tracing is off by default so the demo runs without a collector (the outbound
client still propagates a generated trace id). To enable export to an OTLP
collector, set `tracing.enabled: true` in `config.yaml` and point
`tracing.endpoint` at your collector:

```yaml
tracing:
  enabled: true
  endpoint: localhost:4317
```
