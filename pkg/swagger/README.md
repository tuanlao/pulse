# `pkg/swagger`

Mounts a Swagger UI (`swaggo/gin-swagger`) onto a gin engine. It is **disabled by default** — production services should not expose their API surface unless they opt in. The route mount is gated by config; when disabled, the route is never created and requests return 404. The generated docs package (produced by `swag init`) is still a compile-time import in the application.

## Import
```go
import "github.com/tuanlao/pulse/pkg/swagger"
```

## Configuration
| YAML key (`mapstructure` tag) | Type | Default (from `DefaultConfig`) | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `false` | Toggles mounting the UI. |
| `path` | `string` | `"/swagger"` | Base route for the UI. |
| `host` | `string` | `""` | API host shown in the spec (informational; applied by the app to its `SwaggerInfo`). |
| `base_path` | `string` | `"/"` | API base path shown in the spec, e.g. `/api/v1`. |
| `doc_url` | `string` | `""` | Overrides the `doc.json` URL the UI fetches. Defaults to `<Path>/doc.json`. |
| `instance_name` | `string` | `"swagger"` | Which registered swag spec to serve. |
| `default_models_expand_depth` | `int` | `1` | Model expansion depth; `-1` hides the models section. |
| `persist_authorization` | `bool` | `false` | Keeps auth between page reloads. |

## Usage
```go
//go:generate swag init -g cmd/server/main.go -o docs
import _ "github.com/tuanlao/pulse/docs" // registers the generated swag spec

mounted := swagger.Mount(engine, swagger.DefaultConfig(),
    swagger.WithEnabled(true))
// mounted == true when the UI was registered (cfg.Enabled). UI served at /swagger/*any.
```

## API / Options
- `Mount(engine *gin.Engine, cfg Config, opts ...Option) bool` — registers the UI when `cfg.Enabled` is true (no-op otherwise); returns whether it was mounted. Mounts at `<base>/*any`.
- `DefaultConfig() Config` — defaults (UI disabled).
- Options: `WithEnabled`, `WithPath`, `WithHost`.

## Notes
- Disabled by default; pass `WithEnabled(true)` (or set `enabled: true`) to expose it — typically only in non-production environments.
- You must generate docs with `swag init` and blank-import the generated `docs` package so the swag spec is registered before `Mount` runs; otherwise the UI has no spec to serve.
- The doc URL defaults to `<Path>/doc.json`; override it with `DocURL` when serving the spec from a different location.
- `instance_name` must match the spec instance registered by the generated docs (default `"swagger"`).
