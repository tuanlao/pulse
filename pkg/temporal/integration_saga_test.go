//go:build integration

package temporal_test

import (
	"context"
	"testing"
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"github.com/tuanlao/pulse/pkg/temporal/saga"
)

// threeStepSagaWorkflow: three successful steps, then a failure — compensations
// must run in reverse order (LIFO): undo-3, undo-2, undo-1.
func threeStepSagaWorkflow(ctx workflow.Context) error {
	ctx = fastActivityCtx(ctx)
	s := saga.New(ctx)
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Record", "s1").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-1")
		if err := workflow.ExecuteActivity(ctx, "Record", "s2").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-2")
		if err := workflow.ExecuteActivity(ctx, "Record", "s3").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-3")
		return workflow.ExecuteActivity(ctx, "Fail").Get(ctx, nil)
	})
}

func TestIntegration_Saga_ReverseLIFOCompensation(t *testing.T) {
	tcli, acts, tq := newTemporalHarness(t, threeStepSagaWorkflow)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, threeStepSagaWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err == nil {
		t.Fatal("workflow should fail after the final step errors")
	}

	got := acts.snapshot()
	if len(got) < 3 {
		t.Fatalf("activity log too short: %v", got)
	}
	tail := got[len(got)-3:]
	want := []string{"undo-3", "undo-2", "undo-1"}
	for i := range want {
		if tail[i] != want[i] {
			t.Fatalf("compensation order = %v, want reverse-LIFO %v (full: %v)", tail, want, got)
		}
	}
}

// parallelSagaWorkflow: with WithParallel, compensations run concurrently — both
// must run, in any order.
func parallelSagaWorkflow(ctx workflow.Context) error {
	ctx = fastActivityCtx(ctx)
	s := saga.New(ctx, saga.WithParallel(true))
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Record", "s1").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-1")
		if err := workflow.ExecuteActivity(ctx, "Record", "s2").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-2")
		return workflow.ExecuteActivity(ctx, "Fail").Get(ctx, nil)
	})
}

func TestIntegration_Saga_ParallelCompensation(t *testing.T) {
	tcli, acts, tq := newTemporalHarness(t, parallelSagaWorkflow)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, parallelSagaWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err == nil {
		t.Fatal("workflow should fail")
	}

	got := acts.snapshot()
	has := func(label string) bool {
		for _, v := range got {
			if v == label {
				return true
			}
		}
		return false
	}
	if !has("undo-1") || !has("undo-2") {
		t.Fatalf("both compensations must run in parallel mode, got %v", got)
	}
}

// stopOnCompFailWorkflow: with ContinueOnError=false, a failing compensation halts
// the remaining compensations.
func stopOnCompFailWorkflow(ctx workflow.Context) error {
	ctx = fastActivityCtx(ctx)
	s := saga.New(ctx, saga.WithContinueOnError(false))
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Record", "s1").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-1") // runs LAST in reverse — should be skipped
		if err := workflow.ExecuteActivity(ctx, "Record", "s2").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Fail") // runs FIRST in reverse and fails -> stop
		return workflow.ExecuteActivity(ctx, "Fail").Get(ctx, nil)
	})
}

func TestIntegration_Saga_StopOnCompensationFailure(t *testing.T) {
	tcli, acts, tq := newTemporalHarness(t, stopOnCompFailWorkflow)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, stopOnCompFailWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	_ = run.Get(ctx, nil) // expected to fail

	for _, v := range acts.snapshot() {
		if v == "undo-1" {
			t.Fatalf("undo-1 must be skipped after an earlier compensation failed (ContinueOnError=false): %v", acts.snapshot())
		}
	}
}
