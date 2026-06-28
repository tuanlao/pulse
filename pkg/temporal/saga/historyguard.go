package saga

import "go.temporal.io/sdk/workflow"

// Default Continue-As-New thresholds. A workflow that loops or processes a long
// stream accumulates event history forever; large histories slow replay and grow
// worker memory until it OOMs. Continue-As-New atomically restarts the workflow
// with a fresh (empty) history, carrying state forward. These bounds keep a
// single run's history small.
const (
	defaultMaxEvents = 10000
	defaultMaxBytes  = 20 << 20 // 20 MiB
)

// Thresholds bound a single workflow run's history. It doubles as a config struct
// (mapstructure tags) so a service can declare it in YAML and pass it into the
// workflow input (keeping it deploy-time constant, hence deterministic across
// replays). Zero MaxEvents/MaxBytes fall back to the defaults.
type Thresholds struct {
	// MaxEvents is the history event-count ceiling. Default 10000.
	MaxEvents int `mapstructure:"max_events"`
	// MaxBytes is the history size ceiling in bytes. Default 20 MiB.
	MaxBytes int `mapstructure:"max_bytes"`
	// IgnoreServerSuggestion, when false (the default), also triggers
	// Continue-As-New when the Temporal server suggests it (it does so as history
	// approaches the server-side limits). Set true to rely only on MaxEvents/MaxBytes.
	IgnoreServerSuggestion bool `mapstructure:"ignore_server_suggestion"`
}

// DefaultThresholds returns the default history bounds (respecting the server's
// own Continue-As-New suggestion).
func DefaultThresholds() Thresholds {
	return Thresholds{MaxEvents: defaultMaxEvents, MaxBytes: defaultMaxBytes}
}

func (t Thresholds) withDefaults() Thresholds {
	if t.MaxEvents <= 0 {
		t.MaxEvents = defaultMaxEvents
	}
	if t.MaxBytes <= 0 {
		t.MaxBytes = defaultMaxBytes
	}
	return t
}

// ShouldContinueAsNew reports whether the calling workflow should Continue-As-New
// to keep its history bounded. It MUST be called from workflow code (it reads
// workflow.GetInfo, which is deterministic). The workflow author makes the actual
// call:
//
//	if saga.ShouldContinueAsNew(ctx, thresholds) {
//	    return workflow.NewContinueAsNewError(ctx, MyWorkflow, carriedState)
//	}
//
// When it returns true it bumps a counter on the workflow metrics handler
// (pulse_saga_continue_as_new_suggested) so the OOM guard's activity is visible
// alongside the SDK metrics.
func ShouldContinueAsNew(ctx workflow.Context, t Thresholds) bool {
	t = t.withDefaults()
	info := workflow.GetInfo(ctx)

	should := (!t.IgnoreServerSuggestion && info.GetContinueAsNewSuggested()) ||
		info.GetCurrentHistoryLength() >= t.MaxEvents ||
		info.GetCurrentHistorySize() >= t.MaxBytes

	if should {
		workflow.GetMetricsHandler(ctx).Counter("pulse_saga_continue_as_new_suggested").Inc(1)
	}
	return should
}
