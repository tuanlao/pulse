# pkg/snowflake

Twitter-style 64-bit **Snowflake id** generator with rich conversion helpers and
three pluggable **worker-id** (node) acquisition strategies. The `Generator`
implements `lifecycle.Component`, so it registers into the lifecycle manager like
any other pulse subsystem.

Layout (msb → lsb): `1 sign(=0) | 41 timestamp-ms | NodeBits | StepBits`, with a
custom epoch. Defaults are the canonical Twitter shape: `NodeBits=10` (1024
nodes), `StepBits=12` (4096 ids/ms/node), epoch `2010-11-04` — all per-instance
configurable (unlike `bwmarrin/snowflake`, which uses process-global vars).

```go
import "github.com/tuanlao/pulse/pkg/snowflake"
```

## Worker-id strategies

The worker (node) id uniquely identifies a generator; two generators must never
share one at the same time. `worker_id.strategy` selects how it is obtained:

| Strategy | How | Notes |
|---|---|---|
| `static` | fixed id from `worker_id.static` | default; ideal for local/dev/test |
| `statefulset` | pod ordinal parsed from the pod name (`web-3` → 3) | **errors for a Deployment** (`web-5d4b9c-xk2lp` has no usable number) — strictly safe when available |
| `redis` | pods contend for a unique slot in `[0, pool size)` via a redis lease | works in any topology; lease renewed in the background, **fences** on loss |

The redis strategy is resolved in `Start` (it does I/O); `static` and
`statefulset` are resolved in `New` (no I/O), so those generators are usable
before `Start`.

## Configuration

Nested-object config. Precedence: `DefaultConfig()` < `config.yaml` < `Option`s.

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `true` | When false, a disabled generator whose `Generate` refuses to run |
| `epoch` | int64 (ms) | `1288834974657` | Custom epoch; the 41-bit timestamp counts from here. `0` = default Twitter epoch |
| `node_bits` | uint8 | `10` | Worker-id field width. Bounds the redis pool and max ordinal |
| `step_bits` | uint8 | `12` | Per-ms sequence width. `node_bits + step_bits` must be ≤ 22. `0` = default |
| `max_clock_drift_wait` | duration | `5ms` | How long `Generate` waits for the clock to catch up after it moves backwards |
| `worker_id.strategy` | string | `static` | `static` \| `statefulset` \| `redis` |
| `worker_id.static` | int64 | `0` | Worker id for the static strategy |
| `worker_id.statefulset.pod_name_env` | string | `POD_NAME` | Env var holding the pod name (falls back to `os.Hostname()`) |
| `worker_id.redis.redis.address` | string | `localhost:6379` | Dedicated client address (used when no shared client is passed) |
| `worker_id.redis.redis.{username,password,db}` | | | Redis ACL/auth/db |
| `worker_id.redis.key_prefix` | string | `pulse:snowflake:worker` | Per-slot lock key namespace |
| `worker_id.redis.ttl` | duration | `15s` | Slot lease auto-expiry (frees a crashed pod's slot) |
| `worker_id.redis.renew_interval` | duration | `0` → `ttl/3` | Lease extension cadence |
| `worker_id.redis.max_id` | int | `0` → `2^node_bits` | Pool size override (clamped to the node space) |
| `metrics.enabled` | bool | `true` | Toggle Prometheus metrics |
| `metrics.namespace` / `metrics.subsystem` | string | `pulse` / `snowflake` | Metric naming |

## Usage

```go
gen, err := snowflake.New(cfg.Snowflake, snowflake.Deps{
    Logger:         logger,
    TracerProvider: tracer.Provider(),
    Metrics:        snowflakeMetrics, // from snowflake.NewMetrics(cfg.Snowflake.Metrics, reg)
    RedisClient:    rdb,              // shared client for the redis strategy (optional)
})
if err != nil {
    return err
}
mgr.Register(gen) // after redis, before the HTTP server

// static / statefulset: usable immediately.
id := gen.Generate()
fmt.Println(id.String(), id.Base58(), gen.Node(id), gen.TimeAt(id))

// redis strategy (or any code that must not panic): use the fallible form.
id, err = gen.TryGenerate()
```

### The `ID` type

Layout-independent encoders/decoders (on `ID` / package funcs):
`String`/`ParseString`, `Base2`, `Base32`, `Base36`, `Base58`, `Base64`,
`Bytes`/`ParseBytes`, `IntBytes`/`ParseIntBytes`, `Int64`/`ParseInt64`,
`MarshalJSON`/`UnmarshalJSON` (a **quoted** integer, surviving JS 53-bit
truncation), `MarshalText`/`UnmarshalText`. Base32/Base58 alphabets match
`bwmarrin/snowflake`.

Layout-dependent extractors live on `*Generator` (they need its epoch/bits, since
the package keeps no global layout): `Time(id)`, `TimeAt(id)`, `Node(id)`,
`Step(id)`. Plus `WorkerID()`, `MaxNode()`, `MaxStep()`, `Ready()`.

`Generate() ID` panics if an id cannot be minted (disabled / not started / fenced
/ clock backwards beyond `max_clock_drift_wait`); `TryGenerate() (ID, error)`
returns those as `ErrDisabled` / `ErrNotReady` / `ErrLeaseLost` /
`ErrClockBackwards`.

## Options

`WithStaticWorkerID(id)`, `WithStatefulSetStrategy()`, `WithRedisStrategy(addr)`,
`WithNodeBits(n)`, `WithStepBits(n)`, `WithEpoch(ms)`, `WithEpochTime(t)`,
`WithRedisTTL(d)`, `WithPodNameEnv(name)`, `WithKeyPrefix(prefix)`.

## Metrics

Package-owned registry (`NewMetrics(cfg, reg)`), bounded cardinality (no per-id
labels): `pulse_snowflake_ids_generated_total`, `pulse_snowflake_worker_id`,
`pulse_snowflake_clock_backwards_total`,
`pulse_snowflake_sequence_exhausted_total`,
`pulse_snowflake_worker_id_lease_lost_total`, `pulse_snowflake_fenced`.

## Notes

- **Redis fencing is best-effort under partition.** If a pod loses its lease (a
  partition longer than `ttl`) another pod can briefly hold the same slot before
  the original fences itself and re-acquires a different slot. Fencing + `ttl/3`
  renewal bounds the window but does not make duplicates impossible. When
  available, the `statefulset` strategy is strictly safe.
- **`Start` does bounded redis I/O** (the slot scan) for the redis strategy,
  honoring ctx — like redis `ping_on_start`. An unreachable redis delays startup
  up to `lifecycle.start_timeout`.
- **41-bit timestamp** rolls over ~69 years after the epoch (default → ~2079).
- **Set `POD_NAME`** via the downward API (`metadata.name`) for the statefulset
  strategy; `os.Hostname()` is only a fallback and may differ on host networking.
- **`epoch: 0` and `step_bits: 0`** are read as "use the default" (the zero-value
  convention), so they cannot be selected via config/options.
- **Ordering:** register the generator after `redis` and before the HTTP server,
  so it stops (releasing its redis slot) before the redis client closes.
