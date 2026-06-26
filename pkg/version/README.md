# `pkg/version`

Exposes build metadata — version, commit, and build time — intended to be injected at build time via `-ldflags`. It gives the application a single, importable source of truth for "what build is this?" that can be surfaced in logs, a `/version` endpoint, or `--version` output. Without injection the variables keep dev-friendly defaults so `go test` and local builds still work.

## Import
```go
import "github.com/tuanlao/pulse/pkg/version"
```

## Usage
```go
info := version.Get()
fmt.Printf("version=%s commit=%s built=%s\n",
	info.Version, info.Commit, info.BuildTime)
```

Inject real values at build time:
```sh
go build -ldflags "\
  -X github.com/tuanlao/pulse/pkg/version.Version=1.2.3 \
  -X github.com/tuanlao/pulse/pkg/version.Commit=$(git rev-parse --short HEAD) \
  -X github.com/tuanlao/pulse/pkg/version.BuildTime=$(date -u +%FT%TZ)"
```

## API / Options
- `Version`, `Commit`, `BuildTime` — package-level `string` variables overridden at link time. Defaults: `"dev"`, `"none"`, `"unknown"`.
- `Info` — snapshot struct with JSON tags: `version`, `commit`, `build_time`.
- `Get() Info` — returns the current build metadata as an `Info`.

## Notes
- Values are set with `go build -ldflags "-X ..."`; the linker can only override string variables, which is why all three are `string`.
- When built without the flags (e.g. `go test`), the defaults `dev`/`none`/`unknown` are used.
- `Info` carries `json` tags, so `version.Get()` marshals cleanly for a `/version` HTTP endpoint.
