# `pkg/config`

A thin, generic wrapper around [viper](https://github.com/spf13/viper) for loading configuration into any struct. It is deliberately agnostic of any individual package's `Config` type: callers pre-populate a destination struct with their composed `DefaultConfig()` values and call `Load` to overlay a YAML config file on top. Configuration is object-shaped only — read from a structured YAML file, with no flattened environment variables and no command-line flags. This keeps configuration plumbing centralized while letting each pulse package own its own defaults.

## Import
```go
import "github.com/tuanlao/pulse/pkg/config"
```

## Configuration

`Options` controls how the config file is discovered and decoded. The defaults below come from `DefaultOptions()` and are applied to any zero-valued field by `Load`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `FileName` | `string` | `"config"` | Config file base name (no extension). |
| `FileType` | `string` | `"yaml"` | Config file extension/type. |
| `SearchPaths` | `[]string` | `[".", "./config", "/etc/pulse"]` | Directories searched for the config file. |
| `TagName` | `string` | `"mapstructure"` | Struct tag used for field mapping. |

## Usage
```go
type AppConfig struct {
	HTTP struct {
		ReadTimeout time.Duration `mapstructure:"read_timeout"`
	} `mapstructure:"http"`
}

// Start from your composed defaults.
cfg := AppConfig{}
cfg.HTTP.ReadTimeout = 5 * time.Second

if err := config.Load(&cfg, config.DefaultOptions()); err != nil {
	log.Fatal(err)
}
```

## API / Options
- `Load[T any](dst *T, o Options) error` — overlays the YAML config file onto a non-nil pointer to an already-defaulted struct.
- `Options` — config-file discovery settings (see table).
- `DefaultOptions() Options` — returns `Options` with sensible defaults.

## Notes
- Precedence (low → high): `DefaultConfig()` values < config file (`config.yaml`). Keys absent from the file keep their defaults, because `Unmarshal` only writes keys it actually finds.
- A missing config file is not an error; any other read error is returned.
- Nested structs map to nested YAML/dotted keys (e.g. `http.cors.allow_origins`); `time.Duration` and `time.Time` are treated as leaves and decoded from strings (`StringToTimeDurationHookFunc`). Comma-separated strings decode into slices.
- Fields tagged `-` are skipped.
