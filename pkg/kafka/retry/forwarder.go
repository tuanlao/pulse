package retry

import (
	"context"
	"fmt"

	"github.com/tuanlao/pulse/pkg/kafka/message"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	ktrace "github.com/tuanlao/pulse/pkg/kafka/trace"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ForwardResult describes the outcome of a forward — expressive enough for clean
// logging (which topic, retry vs DLQ, which class).
type ForwardResult struct {
	// Done is true when the record was durably produced (safe to commit the
	// original offset). False means the produce failed and the original must be
	// reprocessed.
	Done bool
	// NextTopic is the destination the record was forwarded to.
	NextTopic string
	// IsDLQ is true when NextTopic is the DLQ.
	IsDLQ bool
	// Class is the error class (set for DLQ forwards).
	Class message.ErrorClass
}

// Forwarder produces retry/DLQ records: it stamps the headers, injects the trace
// context, produces synchronously (so the caller can advance the watermark only
// after the forward is durable), and emits metrics + hooks.
type Forwarder struct {
	cl           *kgo.Client
	group        string
	scopeToGroup bool
	maxReason    int
	metrics      *kmetrics.Metrics
	hooks        message.Hooks
}

// NewForwarder builds a Forwarder.
func NewForwarder(cl *kgo.Client, cfg Config, group string, m *kmetrics.Metrics, hooks message.Hooks) *Forwarder {
	return &Forwarder{
		cl:           cl,
		group:        group,
		scopeToGroup: cfg.ScopeToGroup,
		maxReason:    cfg.MaxErrorReasonBytes,
		metrics:      m,
		hooks:        hooks,
	}
}

// Forward executes the Action for message m (failing with cause). It mutates m's
// headers, produces synchronously, and returns a ForwardResult. A produce error
// yields Done=false so the caller does NOT advance the watermark.
func (f *Forwarder) Forward(ctx context.Context, m *message.Message, a Action, cause error) (ForwardResult, error) {
	origin := originOf(m)

	// Stamp origin coordinates once, so they survive across tier hops.
	if m.Headers.OriginTopic() == "" {
		m.Headers.SetOriginTopic(m.Topic)
		m.Headers.SetOriginPartition(m.Partition)
		m.Headers.SetOriginOffset(m.Offset)
		m.Headers.SetOriginTimestamp(m.Timestamp)
	}
	if cause != nil {
		m.Headers.SetErrorReason(truncate(cause.Error(), f.maxReason))
	}

	if a.IsDLQ {
		m.Headers.SetErrorClass(a.Class.String())
	} else {
		m.Headers.SetRetryCount(a.Attempt)
		m.Headers.SetRetryDueAt(a.DueAt)
		if f.scopeToGroup {
			m.Headers.SetRetryGroup(f.group)
		}
	}

	rec := m.ToRecord(a.Target)
	ktrace.Inject(ctx, rec)

	if err := f.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return ForwardResult{Done: false, NextTopic: a.Target, IsDLQ: a.IsDLQ, Class: a.Class},
			fmt.Errorf("kafka: forward to %q: %w", a.Target, err)
	}

	if a.IsDLQ {
		f.metrics.IncDLQ(origin, f.group, a.Class.String())
		f.hooks.FireDLQ(ctx, m, a.Class.String(), m.Headers.ErrorReason())
	} else {
		f.metrics.IncRetry(origin, f.group)
		f.hooks.FireRetry(ctx, m, a.Attempt, a.Delay)
	}
	return ForwardResult{Done: true, NextTopic: a.Target, IsDLQ: a.IsDLQ, Class: a.Class}, nil
}

// truncate caps s to at most n bytes (so the x-error-reason header never carries
// a whole paragraph / stack trace).
func truncate(s string, n int) string {
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}
