package saga_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tuanlao/pulse/pkg/temporal/saga"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// recorder captures the order activities run in. It is test-side (host) code, so
// a real mutex is fine here (it is NOT inside workflow code).
type recorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *recorder) record(s string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, s)
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// ledger exposes Step (forward) and Undo (compensation) activities. Step fails
// for the account named in failOn.
type ledger struct {
	rec    *recorder
	failOn string
}

func (l *ledger) Step(_ context.Context, name string) error {
	l.rec.record("do:" + name)
	if name == l.failOn {
		return temporal.NewNonRetryableApplicationError("step failed", "StepError", nil)
	}
	return nil
}

func (l *ledger) Undo(_ context.Context, name string) error {
	l.rec.record("undo:" + name)
	return nil
}

func forwardActivityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
}

// transferWF debits "a" then credits "b", registering a compensation after each
// successful step.
func transferWF(ctx workflow.Context) error {
	ctx = forwardActivityCtx(ctx)
	s := saga.New(ctx)
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Step", "a").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Undo", "a")
		if err := workflow.ExecuteActivity(ctx, "Step", "b").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Undo", "b")
		return nil
	})
}

func TestSagaSuccessNoCompensation(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	rec := &recorder{}
	env.RegisterActivity(&ledger{rec: rec})
	env.RegisterWorkflow(transferWF)

	env.ExecuteWorkflow(transferWF)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected workflow error: %v", err)
	}
	if got, want := strings.Join(rec.snapshot(), ","), "do:a,do:b"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

func TestSagaCompensatesInReverse(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	rec := &recorder{}
	// Step "b" fails: only "a"'s compensation has been registered by then.
	env.RegisterActivity(&ledger{rec: rec, failOn: "b"})
	env.RegisterWorkflow(transferWF)

	env.ExecuteWorkflow(transferWF)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if env.GetWorkflowError() == nil {
		t.Fatal("expected a workflow error from the failed step")
	}
	if got, want := strings.Join(rec.snapshot(), ","), "do:a,do:b,undo:a"; got != want {
		t.Fatalf("calls = %q, want %q", got, want)
	}
}

// parallelWF adds two compensations then forces a failure so both run.
func parallelWF(ctx workflow.Context) error {
	ctx = forwardActivityCtx(ctx)
	s := saga.New(ctx, saga.WithParallel(true))
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Step", "a").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Undo", "a")
		if err := workflow.ExecuteActivity(ctx, "Step", "b").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Undo", "b")
		return temporal.NewNonRetryableApplicationError("force rollback", "Forced", nil)
	})
}

func TestSagaParallelCompensation(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	rec := &recorder{}
	env.RegisterActivity(&ledger{rec: rec})
	env.RegisterWorkflow(parallelWF)

	env.ExecuteWorkflow(parallelWF)

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if env.GetWorkflowError() == nil {
		t.Fatal("expected the forced rollback error")
	}
	var undoA, undoB bool
	for _, c := range rec.snapshot() {
		switch c {
		case "undo:a":
			undoA = true
		case "undo:b":
			undoB = true
		}
	}
	if !undoA || !undoB {
		t.Fatalf("expected both compensations to run, got %v", rec.snapshot())
	}
}

// guardWF returns whether the history guard fires for the given thresholds.
func guardWF(ctx workflow.Context, th saga.Thresholds) (bool, error) {
	return saga.ShouldContinueAsNew(ctx, th), nil
}

// ShouldContinueAsNew is driven by the live history length/size and the server's
// suggestion, which the in-memory test environment reports as zero/false — so we
// assert the deterministic "does not fire under high thresholds" path here and
// cover the threshold-comparison logic itself in historyguard_internal_test.go.
func TestShouldContinueAsNewFreshWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(guardWF)
	env.ExecuteWorkflow(guardWF, saga.Thresholds{MaxEvents: 1_000_000, MaxBytes: 1 << 40, IgnoreServerSuggestion: true})

	var should bool
	if err := env.GetWorkflowResult(&should); err != nil {
		t.Fatalf("result: %v", err)
	}
	if should {
		t.Fatal("did not expect Continue-As-New on a fresh workflow under high thresholds")
	}
}
