# Example service

This is the canonical composition root for pulse: a small service that wires the
components together. Pulse has **no** unified `app.Config` — the service owns its
own config struct (`appConfig` in `main.go`), embedding the pulse component
configs it needs plus its own fields, and loads it with `pkg/config`. A small
`normalize()` step then propagates `Env`/`ServiceName`/`Version` into sub-configs
and derives the gin mode from `Env` (via `pkg/env`).

It demonstrates loading `appConfig` from `config.yaml`, building the logger,
tracing and metrics, standing up the HTTP server, creating an outbound HTTP
client pointed back at itself (so the trace id propagates from the inbound
request through the client into headers), and a rueidis redis client showcasing
client-side caching. It also schedules a cron heartbeat job — declared in
`config.yaml` (`cron.jobs.heartbeat`) and bound to its handler in code via
`cronSched.Register("heartbeat", ...)` — and registers everything with the
lifecycle manager for ordered shutdown (tracing first, server last, so the server
drains before tracing flushes; redis and cron stop in between). Server, client,
redis and cron metrics share one Prometheus registry, so a single `/metrics`
endpoint exposes all of them.

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
| `GET /metrics` | Prometheus metrics (server RED + outbound client + redis + cron). |
| `GET /hello/:name` | Returns `hello <name>` with a timestamp. |
| `GET /call/:name` | Calls this service's own `/hello/:name` via the outbound client, propagating the trace id. |
| `GET /cache` | (redis enabled only) Client-side cached read; reports `cache_hit`. |

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

## Redis (client-side caching)

Redis is disabled by default so the demo runs without a server. To try rueidis
client-side caching, start a redis (>= 6, RESP3) and enable it:

```sh
docker run --rm -p 6379:6379 redis:7
```

```yaml
redis:
  enabled: true
  cache:
    enabled: true
    broadcast: # optional: server pushes invalidations per prefix
      enabled: true
      prefixes: ["demo:"]
```

Then call `GET /cache` twice: the first response reports `"cache_hit": false`
(round trip), the second `"cache_hit": true` (served from the local cache) until
the key is invalidated.
