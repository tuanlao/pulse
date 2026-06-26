# `pkg/app`

`pkg/app` is a convenience aggregator that composes every pulse component's
configuration into a single nested-object `Config` (AppConfig) carrying a
deployment `Env`, `ServiceName` and `Version`. Components stay independent and
composable — you can still build each from its own `DefaultConfig` + Options —
but `app.Config` gives a service one structured config to load from
YAML plus a place to express cross-cutting values, and a `Normalize`
step that propagates those values into the sub-configs.

## Import

```go
import "github.com/tuanlao/pulse/pkg/app"
```

## Configuration

`Config` is nested-object shaped: each component embeds its own config object,
so it maps directly onto structured YAML.

| YAML key (`mapstructure` tag) | Type | Default | Description |
| --- | --- | --- | --- |
| `env` | `Env` (string) | `DEV` | Deployment environment: `LOCAL`, `DEV`, `UAT`, `STG`, `PROD` (case-insensitive; unknown/empty → `DEV`). |
| `service_name` | `string` | `""` | Service name; propagated into `tracing.ServiceName` by `Normalize`. |
| `version` | `string` | `""` | Service version; propagated into `tracing.ServiceVersion` by `Normalize`. |
| `log` | `log.Config` | `log.DefaultConfig()` | Logger config (embedded object). |
| `server` | `server.Config` | `server.DefaultConfig()` (with `Mode` cleared) | HTTP server config; `Mode` is left empty so `Normalize` derives the gin mode from `Env`. |
| `tracing` | `tracing.Config` | `tracing.DefaultConfig()` | OpenTelemetry tracing config. |
| `metrics` | `metrics.Config` | `metrics.DefaultConfig()` | Prometheus RED metrics config. |
| `swagger` | `swagger.Config` | `swagger.DefaultConfig()` | Swagger UI config. |
| `cron` | `cron.Config` | `cron.DefaultConfig()` | Cron scheduler config (see `pkg/cron`). |
| `http_clients` | `map[string]client.Config` | `{}` | Named upstream HTTP clients (e.g. `"payment"`, `"user"`); a map so each named client is its own nested object. |
| `lifecycle` | `lifecycle.Config` | `lifecycle.DefaultConfig()` | Ordered startup/shutdown manager config. |

See each component's own package/README for the fields nested under `log`,
`server`, `tracing`, `metrics`, `swagger`, `cron`, `http_clients.*` and
`lifecycle`.

## Usage

```go
cfg := app.DefaultConfig()
cfg.ServiceName = "example-service"
cfg.Version = "1.2.3"

if err := app.Load(&cfg, config.DefaultOptions()); err != nil {
    return err
}

// cfg is now overlaid (defaults < config.yaml) and normalized.
// Build components from the nested sub-configs:
logger, _ := log.New(cfg.Log)
srv, _    := server.New(cfg.Server, server.Deps{ /* ... */ })

// HttpClients is a map of named upstreams.
paymentCfg := cfg.HttpClients["payment"]
```

`Load` does NOT seed defaults itself — `dst` must be pre-populated, typically
with `DefaultConfig()`. It overlays config sources via `pkg/config`, then calls
`Normalize`. (`LoadDefault` is a one-shot convenience that starts from
`DefaultConfig()`, loads, and returns the resolved `Config`.)

`Normalize` (idempotent; called automatically by `Load`) propagates
cross-cutting values:

- normalizes `Env` (upper-cases; unknown/empty → `DEV`);
- copies `ServiceName` → `Tracing.ServiceName` and `Version` →
  `Tracing.ServiceVersion` when those are empty;
- sets `Tracing.Environment` from `Env` when empty;
- derives the gin `Server.Mode` from `Env` when empty: `release` for
  `PROD`/`STG`/`UAT`, `debug` otherwise.

## API / Options / Deps

- `type Config` — the unified AppConfig (fields above).
- `type Env string` with constants `EnvLocal` (`LOCAL`), `EnvDev` (`DEV`),
  `EnvUAT` (`UAT`), `EnvStg` (`STG`), `EnvProd` (`PROD`).
  - `(Env).Valid() bool` — true only for a known environment (no default fallback).
  - `(Env).IsProd() bool` — true when the normalized env is `PROD`.
- `DefaultConfig() Config` — composed defaults (Server.Mode intentionally empty).
- `(*Config).Normalize()` — propagate cross-cutting values; idempotent.
- `Load(dst *Config, o config.Options) error` — overlay sources then normalize;
  `dst` must be non-nil and pre-populated.
- `LoadDefault(o config.Options) (Config, error)` — start from `DefaultConfig()`,
  load, and return the resolved config.

This package has no `Deps` or functional `Option`s of its own — those live on
the individual component packages.

## Notes

- `Load`/`Normalize` do not seed defaults into `dst`; always start from
  `DefaultConfig()` (or call `LoadDefault`), otherwise zero-valued sub-configs
  win.
- `Env` parsing is lenient: unknown or empty values silently become `DEV`. Use
  `(Env).Valid()` if you want to reject unknown values explicitly.
- `Server.Mode` is cleared in `DefaultConfig()` so it follows `Env`; set it
  explicitly only if you want to override the env-derived gin mode.
- `ServiceName`/`Version` only flow into tracing when the corresponding tracing
  fields are still empty — explicit tracing values are never overwritten.
