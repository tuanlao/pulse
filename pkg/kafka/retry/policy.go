package retry

import "github.com/tuanlao/pulse/pkg/kafka/message"

// ReplayPolicy decides whether a DLQ'd message (identified by its error class) is
// worth replaying. This is deliberately a policy — NOT a property of the header
// or envelope — because replayability depends on the class AND on the service
// (e.g. a schema-change may be replayable after a deploy; a validation error
// never is; a business rejection is service-specific). A service can supply its
// own ReplayPolicy.
type ReplayPolicy interface {
	Replayable(class message.ErrorClass) bool
}

// defaultReplayPolicy treats only retries-exhausted (a transient failure that ran
// out of attempts) as replayable; permanent classes are not.
type defaultReplayPolicy struct{}

func (defaultReplayPolicy) Replayable(class message.ErrorClass) bool {
	switch class {
	case message.ErrorRetriesExhausted:
		return true
	default:
		return false
	}
}

// DefaultReplayPolicy is the built-in policy.
var DefaultReplayPolicy ReplayPolicy = defaultReplayPolicy{}

// Replayable reports whether the class is replayable under the default policy.
func Replayable(class message.ErrorClass) bool { return DefaultReplayPolicy.Replayable(class) }
