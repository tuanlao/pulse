package message

import "errors"

// ErrorClass categorizes why a message failed. It is written to the
// x-error-class header on retry/DLQ records. Whether a given class is replayable
// is NOT decided here — that is a policy owned by pkg/kafka/retry (and overridable
// per service), because replayability depends on the class AND on the service.
type ErrorClass string

const (
	// ErrorNonRetryable: the handler declared the failure permanent (e.g. a
	// malformed body) via NonRetryable — routed straight to the DLQ, no retries.
	ErrorNonRetryable ErrorClass = "non_retryable"
	// ErrorRetriesExhausted: a retryable failure that used up every retry tier.
	ErrorRetriesExhausted ErrorClass = "retries_exhausted"

	// Reserved for richer classification by future Classifier rules. Defined so
	// callers reference typed constants instead of bare strings.
	ErrorSchemaChanged    ErrorClass = "schema_changed"
	ErrorPoison           ErrorClass = "poison"
	ErrorValidation       ErrorClass = "validation"
	ErrorExpired          ErrorClass = "expired"
	ErrorBusinessRejected ErrorClass = "business_rejected"
)

// String returns the class as a plain string (for header values / labels).
func (c ErrorClass) String() string { return string(c) }

// nonRetryableError marks an error as permanent so the retry pipeline skips
// straight to the DLQ.
type nonRetryableError struct{ err error }

func (e *nonRetryableError) Error() string { return e.err.Error() }
func (e *nonRetryableError) Unwrap() error { return e.err }

// NonRetryable wraps err to signal that the failure must NOT be retried: the
// message goes straight to the DLQ with class non_retryable. A handler returns
// kafka.NonRetryable(err) for failures that re-processing cannot fix (bad
// payload, validation error, ...). Returns nil when err is nil.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &nonRetryableError{err: err}
}

// IsNonRetryable reports whether err (or anything it wraps) was marked
// NonRetryable.
func IsNonRetryable(err error) bool {
	var n *nonRetryableError
	return errors.As(err, &n)
}
