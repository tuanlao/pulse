package retry

import "github.com/tuanlao/pulse/pkg/kafka/message"

// Classifier maps a handler error to an ErrorClass. An empty class means
// "retryable" (the Planner sends it through the retry tiers); a non-empty class
// is terminal and routes straight to the DLQ.
//
// This is the single place to extend error categorization (e.g. detect schema /
// validation / poison errors) without touching the Planner or the Forwarder.
type Classifier struct{}

// Classify returns the class for err. NonRetryable-wrapped errors are terminal
// (ErrorNonRetryable); everything else is retryable (empty class).
func (Classifier) Classify(err error) message.ErrorClass {
	if err == nil {
		return ""
	}
	if message.IsNonRetryable(err) {
		return message.ErrorNonRetryable
	}
	return ""
}
