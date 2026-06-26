# `pkg/cron`

`pkg/cron` is a configurable job scheduler built on `go-co-op/gocron/v2`. Every
job runs wrapped with structured logging and a tracing span (a trace id is
generated when the context has none), panic recovery, optional per-job
Prometheus metrics, an optional per-job timeout, optional singleton mode, and an
optional cross-pod **redis distributed lock** (each scheduled run is taken by
whichever pod wins the lock, so load is spread across pods). The `Scheduler`
implements `lifecycle.Component` for ordered startup/shutdown. Jobs can be
declared (scheduled) in config and bound to handlers in code, or added
programmatically. Like every pulse package it exposes `Config` +
`DefaultConfig()` + functional `Option`s, with nested-object config (`jobs` /
`lock` / `metrics` sub-objects).

## Import

```go
import "github.com/tuanlao/pulse/pkg/cron"
```

## Configuration

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Toggles the scheduler; when false `Start` is a no-op. |
| `timezone` | `string` | `"UTC"` | IANA location jobs are scheduled in. |
| `stop_timeout` | `time.Duration` | `30s` | Bounds graceful shutdown (waiting for running jobs). |
| `job_timeout` | `time.Duration` | `0` | When > 0, bounds every job run via a context deadline. |
| `singleton` | `bool` | `true` | Prevents a job from overlapping itself within this process (gocron singleton, reschedule mode). |
| `jobs` | `map[string]JobConfig` | `{}` | Jobs declared in config (schedule here, handler bound in code via `Register`). Keyed by job name (see below). |
| `lock` | `LockConfig` | see below | Redis distributed-lock sub-object (cross-pod, opt-in). |
| `metrics` | `MetricsConfig` | see below | Per-job Prometheus metrics sub-object. |

### `jobs.<name>.*` (jobs declared in config)

Declare a job's schedule under `jobs.<name>` and bind its handler in code with
`Scheduler.Register(name, fn)` (call before `Start`). At `Start`, an enabled
config job whose handler is not registered is a fatal error (fail-fast).

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `jobs.<name>.enabled` | `bool` | `false` | Must be true for the job to be scheduled. |
| `jobs.<name>.cron` | `string` | `""` | Crontab spec (mutually exclusive with `every`). |
| `jobs.<name>.with_seconds` | `bool` | `false` | Enables the 6-field crontab format (seconds). |
| `jobs.<name>.every` | `duration` | `0` | Fixed interval; used when `cron` is empty. |
| `jobs.<name>.timeout` | `duration` | `0` | Per-job override of the global `job_timeout` (0 = use global). |

### `lock.*` (redis distributed lock — opt-in)

Off by default so local/dev needs no redis. A redis lock is taken per job run:
across a fleet of pods, each scheduled run is executed by whichever pod acquires
the lock, so runs are spread across pods (load distribution) rather than all
running on every pod. The lock is `pkg/redis`'s rueidis-based mutex
(`redis.Locker`: atomic `SET NX PX` + owner-checked Lua release).

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `lock.enabled` | `bool` | `false` | Toggles the redis distributed lock. |
| `lock.redis.address` | `string` | `"localhost:6379"` | Redis address `host:port`. |
| `lock.redis.username` | `string` | `""` | Username for redis ACL auth. |
| `lock.redis.password` | `string` | `""` | Password for redis auth. |
| `lock.redis.db` | `int` | `0` | Redis database number. |
| `lock.key_prefix` | `string` | `"pulse:cron"` | Namespaces lock keys to avoid cross-service collisions. |
| `lock.tries` | `int` | `1` | Acquire attempts before skipping the run (`1` = skip if another pod holds it). |
| `lock.retry_delay` | `duration` | `100ms` | Wait between acquire attempts when `lock.tries > 1`. |
| `lock.ttl` | `duration` | `30s` | Lock auto-expiry (releases a crashed pod's lock). Should exceed a job's runtime; bound runs with `job_timeout`. |

### `metrics.*` (per-job Prometheus metrics)

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics.enabled` | `bool` | `true` | Toggles metrics. When `false`, `NewMetrics` returns a nil `*CronMetrics`, so wiring it into `Deps` disables per-job metrics. |
| `metrics.namespace` | `string` | `"pulse"` | Prometheus namespace. |
| `metrics.subsystem` | `string` | `"cron"` | Prometheus subsystem. |
| `metrics.buckets` | `[]float64` | `[0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300]` | Run-duration histogram buckets (seconds). |

## Usage

```go
sched, err := cron.New(cfg.Cron, cron.Deps{
    Logger:         logger,
    TracerProvider: tracer.Provider(),
    Metrics:        cronMetrics, // optional; from cron.NewMetrics(...)
})
if err != nil {
    return err
}

// Jobs declared in config (cfg.Cron.Jobs["heartbeat"]): bind the handler by name
// with Register before Start. An enabled config job with no registered handler
// makes Start fail (fail-fast).
sched.Register("heartbeat", func(ctx context.Context) error {
    log.FromContext(ctx, logger).Info("tick") // ctx carries trace_id/span_id
    return nil
})

// Programmatic jobs work alongside config-declared ones.
// Duration-based job.
_, err = sched.AddJob(cron.Every(5*time.Second), "metrics-flush", func(ctx context.Context) error {
    return nil
})

// Crontab-based job (withSeconds enables the 6-field format).
_, err = sched.AddJob(cron.Cron("0 */5 * * * *", true), "report", func(ctx context.Context) error {
    return nil
})

// Scheduler implements lifecycle.Component — register it with the manager.
mgr.Register(sched) // Start() schedules config jobs + begins scheduling (non-blocking); Stop() drains.
```

Each job run is wrapped automatically: a tracing span `cron.<name>` is started
(a trace id is generated and put on the context when tracing is disabled / the
context has none), the request-scoped logger is attached to the context, panics
are recovered and logged, the optional `JobTimeout` applies a context deadline,
and metrics record run count (by `job`/`status`: `success`/`error`/`panic`),
duration, and in-flight gauge. `Singleton` mode (default) prevents a job from
overlapping itself within the process.

The cross-pod redis distributed lock is opt-in (`cfg.Lock.Enabled`). A lock is
taken per job run, so across a fleet each scheduled run is executed by whichever
pod wins the lock — spreading load across pods. Precedence: `Deps.Locker`
(explicit `gocron.Locker`) > `cfg.Lock.Enabled` (otherwise no lock). When enabled,
the rueidis client is `Deps.RedisClient` (shared, e.g. a `*redis.Client` from
`pkg/redis`) or one built from `cfg.Lock.Redis`. A client this package constructs
is owned and closed on `Stop`; a client you pass via `Deps.RedisClient` is not.

## API / Options / Deps

Key exported functions / types:

- `New(cfg Config, deps Deps, opts ...Option) (*Scheduler, error)` — build the scheduler.
- `(*Scheduler).Register(name string, fn JobFunc)` — bind a handler to a config-declared job name; call before `Start`.
- `(*Scheduler).AddJob(def gocron.JobDefinition, name string, fn JobFunc, opts ...gocron.JobOption) (uuid.UUID, error)` — schedule a job programmatically; returns its id.
- `Every(d time.Duration) gocron.JobDefinition` — duration-based definition.
- `Cron(spec string, withSeconds bool) gocron.JobDefinition` — crontab-based definition.
- `type JobFunc = func(ctx context.Context) error` — a scheduled job; returning an error marks the run failed.
- `(*Scheduler).Start/Stop/Name` — implements `lifecycle.Component`.
- `(*Scheduler).Scheduler() gocron.Scheduler` — escape hatch to the underlying gocron scheduler.
- `(*Scheduler).Config() Config` — the resolved config.
- `NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*CronMetrics, error)` — build collectors registered into `reg` (or a fresh registry when nil); `(*CronMetrics).Registry()` returns it.

`Deps` fields (all optional; nil collaborators degrade gracefully):

- `Logger *log.Logger` — job lifecycle logging; nil → no-op logger.
- `TracerProvider trace.TracerProvider` — span per job run; nil → no-op provider.
- `Metrics *CronMetrics` — enables per-job Prometheus metrics; nil disables them.
- `Locker gocron.Locker` — overrides the distributed locker (highest precedence).
- `RedisClient rueidis.Client` — when `lock.enabled`, builds the locker from this shared rueidis client (e.g. a `*redis.Client`) instead of from `cfg.Lock.Redis`.

Functional `Option`s (override `Config`):

- `WithTimezone(tz string)` — set the scheduler timezone.
- `WithJobTimeout(d time.Duration)` — set the per-job context timeout.
- `WithSingleton(enabled bool)` — toggle non-overlapping execution.
- `WithLock(address string)` — enable the redis distributed lock (per-job, spreads runs across pods) at `address`.

## Notes

- Metrics require building a `CronMetrics` (via `NewMetrics`) and passing it in
  `Deps.Metrics`. `NewMetrics` honors `metrics.enabled`: it returns a nil
  `*CronMetrics` when disabled, so the scheduler skips every per-job metric.
- A handler registered with `Register` whose name has no enabled `jobs.*` entry
  is logged at WARN ("registered handler has no enabled config job") so a config
  typo doesn't silently drop the job.
- Share the server's `*prometheus.Registry` with `NewMetrics` so one `/metrics`
  endpoint exposes cron metrics alongside server/client metrics; never use the
  global default registry.
- An invalid `timezone` makes `New` fail — it must be a valid IANA location.
- An enabled `jobs.<name>` whose handler isn't `Register`ed before `Start` makes
  `Start` fail (fail-fast).
- A redis client owned by this package (built from `cfg.Lock.Redis`) is closed
  on `Stop`; one supplied via `Deps.RedisClient` is left for the caller to close.
- `Start` is non-blocking and a no-op when `enabled: false`; `Stop` waits for
  running jobs within `ctx`'s deadline (bounded by `stop_timeout`).
