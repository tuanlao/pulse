package retry

import (
	"time"

	"github.com/tuanlao/pulse/pkg/kafka/message"
)

// Action is the decision the Planner makes for a failed message: where to send
// it next and with what metadata. It is pure data — the Forwarder executes it.
type Action struct {
	// Target is the destination topic (a retry tier or the DLQ).
	Target string
	// IsDLQ is true when Target is the DLQ.
	IsDLQ bool
	// Attempt is the new retry count stamped on a retry forward (0 for DLQ).
	Attempt int
	// DueAt is when a retry record becomes eligible (zero for DLQ).
	DueAt time.Time
	// Delay is the tier backoff (zero for DLQ).
	Delay time.Duration
	// Class is the error class recorded on a DLQ record.
	Class message.ErrorClass
}

// Planner decides the next Action for a failed message. It performs no IO, so it
// is trivially testable.
type Planner struct {
	namer       Namer
	backoffs    []time.Duration
	maxAttempts int
	group       string
}

// NewPlanner builds a Planner from cfg (assumed defaulted) and the consumer group.
func NewPlanner(cfg Config, group string) Planner {
	return Planner{
		namer:       NewNamer(cfg),
		backoffs:    cfg.Backoffs,
		maxAttempts: cfg.MaxAttempts,
		group:       group,
	}
}

// originOf returns the message's origin topic (set once on first failure) or its
// current topic if not yet stamped.
func originOf(m *message.Message) string {
	if o := m.OriginTopic(); o != "" {
		return o
	}
	return m.Topic
}

// Plan decides the Action for message m failing with the given class at time now.
//
//   - A non-empty (terminal) class -> straight to the DLQ.
//   - Otherwise the next attempt; once it would exceed maxAttempts -> the DLQ with
//     class retries_exhausted; else the retry tier for backoffs[attempt-1].
func (p Planner) Plan(m *message.Message, class message.ErrorClass, now time.Time) Action {
	origin := originOf(m)

	if class != "" {
		return Action{Target: p.namer.DLQTopic(origin, p.group), IsDLQ: true, Class: class}
	}

	attempt := m.RetryCount() + 1
	if attempt > p.maxAttempts {
		return Action{
			Target: p.namer.DLQTopic(origin, p.group),
			IsDLQ:  true,
			Class:  message.ErrorRetriesExhausted,
		}
	}

	delay := p.backoffs[attempt-1]
	return Action{
		Target:  p.namer.RetryTopic(origin, delay, p.group),
		Attempt: attempt,
		DueAt:   now.Add(delay),
		Delay:   delay,
	}
}
