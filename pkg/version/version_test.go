package version

import "testing"

func TestGetDefaults(t *testing.T) {
	info := Get()
	if info.Version != Version || info.Commit != Commit || info.BuildTime != BuildTime {
		t.Fatalf("Get() mismatch: %+v", info)
	}
	// Defaults when not built with ldflags.
	if Version == "" || Commit == "" || BuildTime == "" {
		t.Fatalf("default build metadata must be non-empty: %+v", info)
	}
}
