# `pkg/env`

`pkg/env` defines the deployment-environment enum
(`LOCAL`/`DEV`/`UAT`/`STG`/`PROD`) and the small cross-cutting helpers that go
with it. Pulse has **no** god aggregator config — a service owns its own config
struct and composes the component `Config`s it needs — so this leaf package gives
every service a single shared `Env` definition. That way the propagation the old
`app.Normalize` performed (Env → `tracing.Environment`, Env → gin mode) can be
wired explicitly in `main` without copy-pasting the enum.

## Import

```go
import "github.com/tuanlao/pulse/pkg/env"
```

## API

- `type Env string` with constants `EnvLocal`, `EnvDev`, `EnvUAT`, `EnvStg`,
  `EnvProd`.
- `(Env).Valid() bool` — is it a known environment (case-insensitive)? No
  fallback: an unknown value is invalid.
- `(Env).Normalize() Env` — upper-case/trim; unknown or empty → `EnvDev`.
  Idempotent.
- `(Env).IsProd() bool` — true only for `PROD`.
- `(Env).GinMode() string` — `"release"` for PROD/STG/UAT, `"debug"` otherwise.
  Returns gin's mode string (no gin dependency); assign to `server.Config.Mode`.

## Usage

A service embeds `Env` in its own config and wires cross-cutting values
explicitly after loading config:

```go
type Config struct {
    Env         env.Env        `mapstructure:"env"`
    ServiceName string         `mapstructure:"service_name"`
    Version     string         `mapstructure:"version"`
    Server      server.Config  `mapstructure:"server"`
    Tracing     tracing.Config `mapstructure:"tracing"`
    // ...other component configs + service-specific fields
}

cfg.Env = cfg.Env.Normalize()
if cfg.Server.Mode == "" {
    cfg.Server.Mode = cfg.Env.GinMode()
}
if cfg.Tracing.Environment == "" {
    cfg.Tracing.Environment = string(cfg.Env)
}
```

## Notes

- `Normalize` never errors — it coerces unknown/empty values to `EnvDev`. Use
  `Valid` first if you want to reject typos instead of silently defaulting.
- `GinMode` deliberately returns a string rather than importing gin, keeping
  `pkg/env` a dependency-free leaf.
