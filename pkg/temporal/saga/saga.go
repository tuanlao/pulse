// Package saga provides the saga (compensating-transaction) primitive for
// Temporal workflows, plus the Continue-As-New history guard. It runs INSIDE
// workflow code, so everything here is deterministic: it uses only the
// go.temporal.io/sdk/workflow API (no wall clock, no rand, no real mutexes, no
// direct Prometheus client). The Temporal Go SDK has no built-in Saga type (unlike
// the Java SDK); this is the reusable implementation.
//
// A Saga records a compensation after each successful step. If a later step
// fails, Compensate runs the recorded compensations to undo the completed steps —
// by default in reverse (LIFO) order, continuing past individual failures so
// cleanup is best-effort. Both behaviors are configurable.
package saga

import (
	"errors"
	"time"

	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Compensation undoes a completed step. It runs inside the workflow and typically
// calls workflow.ExecuteActivity for the undo activity.
type Compensation func(ctx workflow.Context) error

// Options configures a Saga.
type Options struct {
	// Parallel runs compensations concurrently (via workflow.Go) instead of in
	// reverse-sequential order. Default false (reverse/LIFO — the safe default).
	Parallel bool
	// ContinueOnError keeps compensating the remaining steps even if one fails,
	// aggregating the errors. Default true (best-effort cleanup). When false,
	// Compensate stops at the first failing compensation.
	ContinueOnError bool
	// ActivityOptions are the default activity options applied by
	// AddActivityCompensation (so undo activities are bounded by a timeout + retry).
	ActivityOptions workflow.ActivityOptions
}

// DefaultOptions returns the default saga behavior: reverse-order,
// continue-on-error, with bounded undo activities.
func DefaultOptions() Options {
	return Options{
		Parallel:        false,
		ContinueOnError: true,
		ActivityOptions: workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		},
	}
}

// Option overrides Options fields.
type Option func(*Options)

// WithParallel toggles concurrent compensation.
func WithParallel(b bool) Option { return func(o *Options) { o.Parallel = b } }

// WithContinueOnError toggles best-effort (vs stop-at-first-failure) compensation.
func WithContinueOnError(b bool) Option { return func(o *Options) { o.ContinueOnError = b } }

// WithActivityOptions sets the default activity options for AddActivityCompensation.
func WithActivityOptions(ao workflow.ActivityOptions) Option {
	return func(o *Options) { o.ActivityOptions = ao }
}

// Saga collects compensations for a workflow and runs them on failure.
type Saga struct {
	opts          Options
	compensations []Compensation
	log           sdklog.Logger
	compensated   bool
}

// New binds a Saga to the workflow context.
func New(ctx workflow.Context, opts ...Option) *Saga {
	o := DefaultOptions()
	for _, fn := range opts {
		fn(&o)
	}
	return &Saga{opts: o, log: workflow.GetLogger(ctx)}
}

// AddCompensation records a raw compensation closure. Compensations run LIFO
// (most recently added first) unless Parallel is set.
func (s *Saga) AddCompensation(c Compensation) {
	s.compensations = append(s.compensations, c)
}

// AddActivityCompensation records "run this undo activity" using the saga's
// default ActivityOptions. It is the common case: register it right after the
// forward step it undoes.
func (s *Saga) AddActivityCompensation(activity interface{}, args ...interface{}) {
	s.AddActivityCompensationWithOptions(s.opts.ActivityOptions, activity, args...)
}

// AddActivityCompensationWithOptions records an undo activity with explicit
// activity options.
func (s *Saga) AddActivityCompensationWithOptions(ao workflow.ActivityOptions, activity interface{}, args ...interface{}) {
	s.compensations = append(s.compensations, func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, ao)
		return workflow.ExecuteActivity(ctx, activity, args...).Get(ctx, nil)
	})
}

// Compensate runs the recorded compensations and returns the aggregated error.
// It is idempotent (a second call is a no-op).
func (s *Saga) Compensate(ctx workflow.Context) error {
	if s.compensated {
		return nil
	}
	s.compensated = true
	if len(s.compensations) == 0 {
		return nil
	}
	if s.opts.Parallel {
		return s.compensateParallel(ctx)
	}
	return s.compensateSequential(ctx)
}

func (s *Saga) compensateSequential(ctx workflow.Context) error {
	var errs []error
	for i := len(s.compensations) - 1; i >= 0; i-- {
		if err := s.compensations[i](ctx); err != nil {
			s.log.Error("saga compensation failed", "index", i, "error", err)
			errs = append(errs, err)
			if !s.opts.ContinueOnError {
				break
			}
		}
	}
	return errors.Join(errs...)
}

func (s *Saga) compensateParallel(ctx workflow.Context) error {
	n := len(s.compensations)
	errsList := make([]error, n)
	wg := workflow.NewWaitGroup(ctx)
	for i := n - 1; i >= 0; i-- {
		wg.Add(1)
		workflow.Go(ctx, func(gctx workflow.Context) {
			defer wg.Done()
			if err := s.compensations[i](gctx); err != nil {
				errsList[i] = err
				s.log.Error("saga compensation failed", "index", i, "error", err)
			}
		})
	}
	wg.Wait(ctx)
	return errors.Join(errsList...)
}

// Run executes fn and, if it returns an error, runs the recorded compensations.
// It returns errors.Join(fnErr, compensateErr) so callers see both the original
// failure and any compensation problems.
func (s *Saga) Run(ctx workflow.Context, fn func() error) error {
	if err := fn(); err != nil {
		if compErr := s.Compensate(ctx); compErr != nil {
			return errors.Join(err, compErr)
		}
		return err
	}
	return nil
}
