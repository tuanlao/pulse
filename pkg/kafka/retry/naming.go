package retry

import (
	"strconv"
	"strings"
	"time"
)

// Namer builds retry-tier and DLQ topic names from the configured patterns.
type Namer struct {
	retryPattern string
	dlqPattern   string
	delayFormat  string
	backoffs     []time.Duration
}

// NewNamer builds a Namer from cfg (which is assumed defaulted).
func NewNamer(cfg Config) Namer {
	return Namer{
		retryPattern: cfg.RetrySuffixPattern,
		dlqPattern:   cfg.DLQSuffixPattern,
		delayFormat:  cfg.DelayFormat,
		backoffs:     cfg.Backoffs,
	}
}

// FormatDelay renders a delay for the {delay} token: "human" (5s, 10s, 1m, 90s)
// or "ms" (5000, 60000). A whole number of hours/minutes/seconds renders with the
// largest single unit; anything finer falls back to time.Duration.String().
func FormatDelay(d time.Duration, format string) string {
	if format == "ms" {
		return strconv.FormatInt(d.Milliseconds(), 10)
	}
	switch {
	case d >= time.Hour && d%time.Hour == 0:
		return strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d >= time.Minute && d%time.Minute == 0:
		return strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	case d >= time.Second && d%time.Second == 0:
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	default:
		return d.String()
	}
}

// RetryTopic builds the retry-tier topic for an origin, delay, and group.
func (n Namer) RetryTopic(origin string, delay time.Duration, group string) string {
	r := strings.NewReplacer(
		"{origin}", origin,
		"{delay}", FormatDelay(delay, n.delayFormat),
		"{group}", group,
	)
	return r.Replace(n.retryPattern)
}

// RetryTopics builds every retry-tier topic for an origin (one per backoff),
// used by the consumer to build its subscription.
func (n Namer) RetryTopics(origin, group string) []string {
	out := make([]string, 0, len(n.backoffs))
	for _, d := range n.backoffs {
		out = append(out, n.RetryTopic(origin, d, group))
	}
	return out
}

// DLQTopic builds the DLQ topic for an origin.
func (n Namer) DLQTopic(origin, group string) string {
	r := strings.NewReplacer("{origin}", origin, "{group}", group)
	return r.Replace(n.dlqPattern)
}
