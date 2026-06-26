// Package version exposes build metadata that is intended to be injected at
// build time via -ldflags, e.g.:
//
//	go build -ldflags "\
//	  -X github.com/tuanlao/pulse/pkg/version.Version=1.2.3 \
//	  -X github.com/tuanlao/pulse/pkg/version.Commit=$(git rev-parse --short HEAD) \
//	  -X github.com/tuanlao/pulse/pkg/version.BuildTime=$(date -u +%FT%TZ)"
package version

// These variables are overridden at link time via -ldflags. They keep their
// default values when built without the flags (e.g. `go test`).
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

// Info is a snapshot of the build metadata.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// Get returns the current build metadata.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildTime: BuildTime,
	}
}
