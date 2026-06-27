// Package retry implements the non-blocking retry-topic model (à la Spring Kafka)
// plus the blocking in-place model, with a clean separation of responsibilities:
//
//   - Classifier turns a handler error into an ErrorClass.
//   - Planner (pure, IO-free) turns (message, class) into an Action: which retry
//     tier or DLQ, the attempt number, and the due time.
//   - Forwarder performs the Action: it builds the record, produces it, and emits
//     metrics + hooks.
//   - DelayScheduler enforces a retry record's due time via partition pause +
//     seek-back, so the consumer stays ignorant of pause/seek/timer mechanics.
//   - ReplayPolicy decides whether a DLQ'd class is replayable (policy, not data).
//
// Non-blocking retries forward a failed message to a per-delay retry topic
// (origin.retry.5s -> .10s -> .1m), exhausting into the DLQ (origin.dlq). The
// blocking model retries in place (preserving order) and is used by the ordered
// and key_ordered consumer modes.
package retry

import "time"

// Config configures retry behavior.
type Config struct {
	// Enabled toggles retries. When false, a failed message goes straight to the
	// DLQ (if enabled). Default true.
	Enabled bool `mapstructure:"enabled"`
	// Strategy is "auto" (ordered/key_ordered -> blocking, unordered ->
	// non_blocking), "blocking", or "non_blocking". Default "auto".
	Strategy string `mapstructure:"strategy"`
	// Backoffs are the per-tier delays; their count is the default attempt cap.
	// Default [5s, 10s, 1m].
	Backoffs []time.Duration `mapstructure:"backoffs"`
	// MaxAttempts caps non-blocking retry forwards. 0 (or > len(Backoffs)) means
	// len(Backoffs).
	MaxAttempts int `mapstructure:"max_attempts"`
	// BlockingMaxAttempts caps blocking in-place retries. 0 means len(Backoffs).
	BlockingMaxAttempts int `mapstructure:"blocking_max_attempts"`
	// ScopeToGroup, when true, stamps the producing group on retry records so only
	// that group reprocesses them. When false (default) the retry-group header is
	// left empty and any group consumes the retry.
	ScopeToGroup bool `mapstructure:"scope_to_group"`
	// RetrySuffixPattern templates the retry topic name. Tokens: {origin}, {delay},
	// {group}. Default "{origin}.retry.{delay}".
	RetrySuffixPattern string `mapstructure:"retry_suffix_pattern"`
	// DLQSuffixPattern templates the DLQ topic name. Tokens: {origin}, {group}.
	// Default "{origin}.dlq".
	DLQSuffixPattern string `mapstructure:"dlq_suffix_pattern"`
	// DelayFormat is "human" (5s, 1m) or "ms" (5000, 60000) for the {delay} token.
	// Default "human".
	DelayFormat string `mapstructure:"delay_format"`
	// MaxErrorReasonBytes truncates the x-error-reason header so it never carries a
	// whole stack trace (full detail goes to the log). Default 256.
	MaxErrorReasonBytes int `mapstructure:"max_error_reason_bytes"`

	DLQ DLQConfig `mapstructure:"dlq"`
}

// DLQConfig configures the dead-letter queue.
type DLQConfig struct {
	// Enabled toggles the DLQ. When false, an exhausted/non-retryable message is
	// dropped. Default true.
	Enabled bool `mapstructure:"enabled"`
}

// DefaultConfig returns retry defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:             true,
		Strategy:            "auto",
		Backoffs:            []time.Duration{5 * time.Second, 10 * time.Second, time.Minute},
		ScopeToGroup:        false,
		RetrySuffixPattern:  "{origin}.retry.{delay}",
		DLQSuffixPattern:    "{origin}.dlq",
		DelayFormat:         "human",
		MaxErrorReasonBytes: 256,
		DLQ:                 DLQConfig{Enabled: true},
	}
}

// ApplyDefaults fills empty fields and resolves the attempt caps.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if len(c.Backoffs) == 0 {
		c.Backoffs = d.Backoffs
	}
	if c.Strategy == "" {
		c.Strategy = d.Strategy
	}
	if c.RetrySuffixPattern == "" {
		c.RetrySuffixPattern = d.RetrySuffixPattern
	}
	if c.DLQSuffixPattern == "" {
		c.DLQSuffixPattern = d.DLQSuffixPattern
	}
	if c.DelayFormat == "" {
		c.DelayFormat = d.DelayFormat
	}
	if c.MaxErrorReasonBytes <= 0 {
		c.MaxErrorReasonBytes = d.MaxErrorReasonBytes
	}
	// A non-blocking run can retry at most once per tier.
	if c.MaxAttempts <= 0 || c.MaxAttempts > len(c.Backoffs) {
		c.MaxAttempts = len(c.Backoffs)
	}
	if c.BlockingMaxAttempts <= 0 {
		c.BlockingMaxAttempts = len(c.Backoffs)
	}
}

// Strategy constants.
const (
	StrategyAuto        = "auto"
	StrategyBlocking    = "blocking"
	StrategyNonBlocking = "non_blocking"
)
