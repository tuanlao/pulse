# `pkg/redis`

`pkg/redis` is a configurable redis component built on
[rueidis](https://github.com/redis/rueidis). Its headline feature is rueidis
**client-side caching**, including the cheap, scalable **broadcast (BCAST) prefix**
tracking mode — both fully declarable in config. A single `addresses` list covers
standalone (one address) and cluster (many, auto-discovered) topologies; Sentinel
and TLS are opt-in sub-objects. The `Client` embeds `rueidis.Client` (so callers
use `B()`/`Do`/`DoCache` directly) and implements `lifecycle.Component` for
ordered startup/shutdown. Commands are optionally wrapped with OTel spans and
**package-owned Prometheus metrics** (never the OTel→Prometheus bridge). Like
every pulse package it exposes `Config` + `DefaultConfig()` + functional
`Option`s, with nested-object config (`tls`/`cache`/`sentinel`/`metrics`
sub-objects).

## Import

```go
import "github.com/tuanlao/pulse/pkg/redis"
```

## Configuration

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Toggles the component; when false `New` returns a disabled client that never dials and whose lifecycle methods are no-ops. |
| `addresses` | `[]string` | `["localhost:6379"]` | Redis nodes (`host:port`). One = standalone; many = cluster (auto-discovered). With Sentinel, these address the sentinels. |
| `username` | `string` | `""` | Username for redis ACL auth. |
| `password` | `string` | `""` | Password for redis auth. |
| `db` | `int` | `0` | Database number (`SELECT`); ignored in cluster mode. |
| `client_name` | `string` | `""` | `CLIENT SETNAME` for server-side observability. |
| `dial_timeout` | `duration` | `5s` | Bounds establishing a TCP connection. |
| `conn_write_timeout` | `duration` | `10s` | Per-connection read/write timeout (also drives the periodic PING liveness check). |
| `send_to_replicas` | `bool` | `false` | Route read-only commands to replicas (cluster mode). |
| `ping_on_start` | `bool` | `true` | `Start` issues a PING as a readiness gate (fail fast on a broken connection). |
| `blocking_pool_size` | `int` | `0` | Pool size for blocking commands; 0 = rueidis default. |
| `pipeline_multiplex` | `int` | `0` | TCP connections used to pipeline; 0 = rueidis default, `-1` disables. |
| `ring_scale_each_conn` | `int` | `0` | Ring buffer scale per connection; 0 = rueidis default. |
| `max_flush_delay` | `duration` | `0` | When > 0, micro-batches pipeline writes (throughput vs latency). |
| `tls` | `TLSConfig` | see below | TLS sub-object (opt-in). |
| `cache` | `CacheConfig` | see below | Client-side caching sub-object (the headline feature). |
| `sentinel` | `SentinelConfig` | see below | Redis Sentinel sub-object (opt-in). |
| `tracing` | `TracingConfig` | see below | Per-command OTel spans. |
| `metrics` | `MetricsConfig` | see below | Per-command Prometheus metrics. |

### `cache.*` (client-side caching — the headline feature)

Caching requires RESP3 (redis ≥ 6). It is on by default: reads issued via
`DoCache` are served from a per-connection local cache until the server
invalidates them, avoiding a round trip. **Broadcast (BCAST)** mode is the cheap,
scalable variant — the server proactively pushes invalidations for the configured
key prefixes instead of tracking individual keys.

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `cache.enabled` | `bool` | `true` | Toggles client-side caching. When false, `DoCache` transparently falls back to a normal round trip (`ClientOption.DisableCache`). |
| `cache.size_per_conn` | `int` | `0` | Cache size in bytes bound to each connection; 0 = rueidis default (128 MiB). |
| `cache.broadcast.enabled` | `bool` | `false` | Enables `CLIENT TRACKING ... BCAST`. Requires at least one prefix. Off by default because prefixes are application-specific. |
| `cache.broadcast.prefixes` | `[]string` | `[]` | Key prefixes the server broadcasts invalidations for (e.g. `["user:", "product:"]`). Required when `broadcast.enabled`. |

### `sentinel.*` (opt-in)

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `sentinel.enabled` | `bool` | `false` | Toggles Sentinel mode; `addresses` then point at the sentinels. |
| `sentinel.master_set` | `string` | `"mymaster"` (when enabled) | Monitored master set name. |
| `sentinel.username` | `string` | `""` | Username to authenticate to the sentinels. |
| `sentinel.password` | `string` | `""` | Password to authenticate to the sentinels. |

### `tls.*` (opt-in)

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `tls.enabled` | `bool` | `false` | Toggles TLS. |
| `tls.insecure_skip_verify` | `bool` | `false` | Disables certificate verification (dev only). |
| `tls.ca_file` | `string` | `""` | PEM bundle to verify the server certificate against. |
| `tls.cert_file` / `tls.key_file` | `string` | `""` | Client certificate for mutual TLS. |
| `tls.server_name` | `string` | `""` | Overrides the SNI / verification hostname. |

### `metrics.*` (per-command Prometheus metrics)

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics.enabled` | `bool` | `true` | Toggles metrics. When false, `NewMetrics` returns a nil `*Metrics`, so wiring it into `Deps` disables per-command metrics. |
| `metrics.namespace` | `string` | `"pulse"` | Prometheus namespace. |
| `metrics.subsystem` | `string` | `"redis"` | Prometheus subsystem. |
| `metrics.buckets` | `[]float64` | `[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5]` | Command-duration histogram buckets (seconds). |

Metrics exposed (labels use the command **verb** only — e.g. `GET` — to bound
cardinality; pipelines collapse to `PIPELINE`):

- `<ns>_<sub>_command_duration_seconds{command,status}` — duration histogram.
- `<ns>_<sub>_commands_total{command,status}` — command counter (status `ok`/`error`).
- `<ns>_<sub>_cache_total{command,result}` — `DoCache` lookups by `result` (`hit`/`miss`).

### `tracing.*`

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `tracing.enabled` | `bool` | `true` | Per-command spans `redis.<verb>` (requires a non-nil `Deps.TracerProvider` to export). |

## Usage

```go
// Build per-command metrics sharing the server's registry (so /metrics exposes
// redis metrics alongside the rest); pass nil to disable.
redisMetrics, err := redis.NewMetrics(cfg.Redis.Metrics, red.Registry())
if err != nil {
    return err
}

rdb, err := redis.New(cfg.Redis, redis.Deps{
    Logger:         logger,
    TracerProvider: tracer.Provider(),
    Metrics:        redisMetrics,
})
if err != nil {
    return err
}

// Client implements lifecycle.Component — register it after tracing and before
// the HTTP server so it closes after the server drains.
mgr.Register(rdb)

// Commands use the embedded rueidis API directly.
if err := rdb.Do(ctx, rdb.B().Set().Key("user:42").Value("alice").Build()).Error(); err != nil {
    return err
}

// Client-side cached read: the second call within the TTL is served locally
// (no round trip) until the key is invalidated.
name, err := rdb.DoCache(ctx, rdb.B().Get().Key("user:42").Cache(), time.Minute).ToString()
```

To enable broadcast caching for hot, well-prefixed keys, either set
`cache.broadcast` in YAML or use the option:

```go
rdb, err := redis.New(cfg.Redis, deps, redis.WithBroadcast("user:", "product:"))
```

## Distributed lock (mutex)

`Locker` is a redis distributed mutex built on the same rueidis client.
Acquisition is an atomic `SET key token NX PX <ttl>`; **release and extend are
owner-checked Lua scripts** (`if GET == token then DEL/PEXPIRE`), so one owner can
never drop or prolong another's lock. `pkg/cron` uses it for its cross-pod job
lock.

```go
locker := redis.NewLocker(rdb.Client, // any rueidis.Client (e.g. the embedded one)
    redis.WithLockKeyPrefix("pulse:job"),
    redis.WithLockTTL(30*time.Second),
    redis.WithLockTries(1), // 1 = skip if held; >1 retries with WithLockRetryDelay
)

lock, err := locker.Lock(ctx, "nightly-report")
if errors.Is(err, redis.ErrLockNotAcquired) {
    return // held by another owner — skip
}
if err != nil {
    return err
}
defer lock.Unlock(ctx)

// optional: re-check ownership / extend for long critical sections
if ok, _ := lock.Valid(ctx); ok { _ = lock.Extend(ctx) }
```

- `NewLocker(client rueidis.Client, opts ...LockerOption) *Locker`
- `(*Locker).Lock(ctx, name) (*Lock, error)` — returns `ErrLockNotAcquired` when held.
- `(*Lock).Unlock/Extend(ctx) error` — owner-checked; return `ErrLockNotHeld` if lost.
- `(*Lock).Valid(ctx) (bool, error)` — does this owner still hold it?
- Options: `WithLockKeyPrefix`, `WithLockTTL`, `WithLockTries`, `WithLockRetryDelay`.

The `TTL` is a safety net for crashed owners — it must exceed the critical
section, or refresh it with `Extend` (the lock has no built-in watchdog).

## API / Options / Deps

Key exported functions / types:

- `New(cfg Config, deps Deps, opts ...Option) (*Client, error)` — build the client.
- `type Client struct { rueidis.Client; ... }` — embeds the rueidis client; all
  command methods (`B`, `Do`, `DoCache`, `DoMultiCache`, ...) are available directly.
- `(*Client).Start/Stop/Name` — implements `lifecycle.Component`.
- `(*Client).CheckReady(ctx)` — implements `lifecycle.ReadinessChecker` (PING).
- `(*Client).Config() Config` — the resolved config.
- `NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*Metrics, error)` —
  build collectors registered into `reg` (or a fresh registry when nil);
  `(*Metrics).Registry()` returns it.

`Deps` fields (all optional; nil collaborators degrade gracefully):

- `Logger *log.Logger` — lifecycle logging; nil → no-op logger.
- `TracerProvider trace.TracerProvider` — per-command spans; nil → no-op provider.
- `Metrics *Metrics` — per-command Prometheus metrics; nil disables them.

Functional `Option`s (override `Config`):

- `WithAddresses(addrs ...string)` — redis node / sentinel addresses.
- `WithCredentials(username, password string)` — auth.
- `WithDB(db int)` / `WithClientName(name string)`.
- `WithClientCache(enabled bool)` — toggle client-side caching.
- `WithBroadcast(prefixes ...string)` — enable BCAST-mode caching for prefixes.
- `WithSendToReplicas(enabled bool)` — route read-only commands to replicas.
- `WithSentinel(masterSet string)` — enable Sentinel mode.
- `WithTLS(tls TLSConfig)` — set the TLS sub-config.

## Notes

- **Client-side caching needs RESP3 (redis ≥ 6).** Against an older server, set
  `cache.enabled: false` (rueidis otherwise refuses to start without
  `DisableCache`).
- The client dials in `New` (so a connection error surfaces there); `Start` only
  issues the optional readiness PING. `Stop` closes the client (synchronous).
- A disabled client (`enabled: false`) never dials and its lifecycle methods are
  no-ops — do not issue commands on it.
- Metric/span labels use the command **verb** only, never keys; multi/pipeline
  calls collapse to a single `PIPELINE` label. A redis-nil reply (missing key) is
  a normal outcome, not an `error`.
- Share the server's `*prometheus.Registry` with `NewMetrics` so one `/metrics`
  endpoint exposes redis metrics alongside server/client/cron; never use the
  global default registry.
- `pkg/cron`'s distributed lock uses this package's `Locker`; the whole library
  is unified on rueidis (no `go-redis`).
