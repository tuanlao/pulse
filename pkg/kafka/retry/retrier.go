package retry

import (
	"context"
	"time"

	"github.com/tuanlao/pulse/pkg/kafka/message"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Retrier is the thin orchestrator that wires Classifier -> Planner -> Forwarder
// (and the blocking in-place loop). The consumer holds one and calls Forward (for
// the non-blocking model) or RunInPlace (for the blocking model).
type Retrier struct {
	cfg        Config
	group      string
	classifier Classifier
	planner    Planner
	forwarder  *Forwarder
	namer      Namer
}

// NewRetrier builds a Retrier over the consumer's (produce-capable) client.
func NewRetrier(cfg Config, group string, cl *kgo.Client, m *kmetrics.Metrics, hooks message.Hooks) *Retrier {
	cfg.ApplyDefaults()
	return &Retrier{
		cfg:        cfg,
		group:      group,
		classifier: Classifier{},
		planner:    NewPlanner(cfg, group),
		forwarder:  NewForwarder(cl, cfg, group, m, hooks),
		namer:      NewNamer(cfg),
	}
}

// EffectiveStrategy resolves the strategy for a consumer mode.
func (r *Retrier) EffectiveStrategy(mode string) string {
	return EffectiveStrategy(r.cfg, mode)
}

// EffectiveStrategy resolves the strategy for a consumer mode without needing a
// Retrier (the consumer calls it before building its client). "auto" maps
// unordered -> non-blocking (retry topics) and ordered/key_ordered -> blocking
// (in-place, order-preserving).
func EffectiveStrategy(cfg Config, mode string) string {
	switch cfg.Strategy {
	case StrategyBlocking, StrategyNonBlocking:
		return cfg.Strategy
	default:
		if mode == "unordered" {
			return StrategyNonBlocking
		}
		return StrategyBlocking
	}
}

// Namer returns the topic namer (used by the consumer to build its subscription).
func (r *Retrier) Namer() Namer { return r.namer }

// Forward routes a failed message to its next retry tier or the DLQ (the
// non-blocking model). The returned ForwardResult.Done reports whether the
// original offset is safe to advance.
func (r *Retrier) Forward(ctx context.Context, m *message.Message, cause error) (ForwardResult, error) {
	class := r.classifier.Classify(cause)

	var a Action
	switch {
	case !r.cfg.Enabled && class == "":
		// Retries off: a retryable failure goes straight to the DLQ.
		a = Action{Target: r.namer.DLQTopic(originOf(m), r.group), IsDLQ: true, Class: message.ErrorRetriesExhausted}
	default:
		a = r.planner.Plan(m, class, time.Now())
	}

	if a.IsDLQ && !r.cfg.DLQ.Enabled {
		// DLQ disabled: drop, but the offset is handled (safe to advance).
		return ForwardResult{Done: true, IsDLQ: true, Class: a.Class}, nil
	}
	return r.forwarder.Forward(ctx, m, a, cause)
}

// RunInPlace runs the handler with blocking in-place retries (order-preserving,
// for the ordered / key_ordered modes): it re-invokes the handler after each
// backoff up to BlockingMaxAttempts, then routes to the DLQ. It returns the final
// handler error (nil on success) and whether the offset is safe to advance
// (false only when a DLQ produce failed or ctx was cancelled mid-backoff).
func (r *Retrier) RunInPlace(ctx context.Context, m *message.Message, handler message.Handler) (handlerErr error, advance bool) {
	err := handler(ctx, m)
	if err == nil {
		return nil, true
	}
	if message.IsNonRetryable(err) {
		return err, r.dlqAdvance(ctx, m, message.ErrorNonRetryable, err)
	}

	if r.cfg.Enabled {
		for attempt := 1; attempt <= r.cfg.BlockingMaxAttempts; attempt++ {
			if !sleepCtx(ctx, r.backoffAt(attempt-1)) {
				return ctx.Err(), false
			}
			err = handler(ctx, m)
			if err == nil {
				return nil, true
			}
			if message.IsNonRetryable(err) {
				return err, r.dlqAdvance(ctx, m, message.ErrorNonRetryable, err)
			}
		}
	}
	return err, r.dlqAdvance(ctx, m, message.ErrorRetriesExhausted, err)
}

// dlqAdvance forwards to the DLQ and reports whether the offset is safe to
// advance (true when DLQ'd durably or dropped; false when the produce failed).
func (r *Retrier) dlqAdvance(ctx context.Context, m *message.Message, class message.ErrorClass, cause error) bool {
	if !r.cfg.DLQ.Enabled {
		return true // dropped
	}
	a := Action{Target: r.namer.DLQTopic(originOf(m), r.group), IsDLQ: true, Class: class}
	res, err := r.forwarder.Forward(ctx, m, a, cause)
	return res.Done && err == nil
}

func (r *Retrier) backoffAt(i int) time.Duration {
	if len(r.cfg.Backoffs) == 0 {
		return 0
	}
	if i >= len(r.cfg.Backoffs) {
		return r.cfg.Backoffs[len(r.cfg.Backoffs)-1]
	}
	return r.cfg.Backoffs[i]
}

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
