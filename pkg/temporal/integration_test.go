//go:build integration

// Integration tests for the temporal saga subsystem against a REAL Temporal
// server. They run a two-step saga workflow on a real worker and assert both the
// happy path and the compensating-transaction (rollback) path.
//
// They need a live Temporal server. Run with the docker-compose stack:
//
//	make infra-up
//	go test -race -tags=integration ./pkg/temporal/... -run TestIntegration -v
//
// The frontend address comes from TEMPORAL_HOSTPORT (default localhost:7233). Each
// test isolates itself with a uuid-suffixed task queue + its own worker instance.
package temporal_test

import (
	"context"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/temporal"
	"github.com/tuanlao/pulse/pkg/temporal/saga"
	sdkclient "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

func temporalHostPort() string {
	if v := os.Getenv("TEMPORAL_HOSTPORT"); v != "" {
		return v
	}
	return "localhost:7233"
}

// sagaActivities holds per-test state. Registering the struct exposes its methods
// as activities named "Record" and "Fail"; the recorder lets the test assert the
// exact order activities (including compensations) ran in.
type sagaActivities struct {
	mu     sync.Mutex
	events []string
}

func (a *sagaActivities) Record(_ context.Context, label string) error {
	a.mu.Lock()
	a.events = append(a.events, label)
	a.mu.Unlock()
	return nil
}

func (a *sagaActivities) Fail(_ context.Context) error {
	return sdktemporal.NewNonRetryableApplicationError("step failed", "stepFailure", nil)
}

func (a *sagaActivities) snapshot() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.events))
	copy(out, a.events)
	return out
}

// fastActivityCtx bounds activities to a single attempt so a deliberate failure
// surfaces immediately instead of being retried.
func fastActivityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: 1},
	})
}

// happySagaWorkflow: debit then credit, both succeed -> no compensation.
func happySagaWorkflow(ctx workflow.Context) error {
	ctx = fastActivityCtx(ctx)
	s := saga.New(ctx)
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Record", "debit").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-debit")
		if err := workflow.ExecuteActivity(ctx, "Record", "credit").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-credit")
		return nil
	})
}

// compensateSagaWorkflow: debit succeeds, the next step fails -> the recorded
// compensation (undo-debit) runs in reverse.
func compensateSagaWorkflow(ctx workflow.Context) error {
	ctx = fastActivityCtx(ctx)
	s := saga.New(ctx)
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, "Record", "debit").Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation("Record", "undo-debit")
		if err := workflow.ExecuteActivity(ctx, "Fail").Get(ctx, nil); err != nil {
			return err // triggers Compensate -> undo-debit
		}
		s.AddActivityCompensation("Record", "undo-credit")
		return nil
	})
}

// newTemporalHarness wires a client + worker on a fresh task queue, registers the
// activities, and starts both (worker after the workflow is registered). It
// returns the client, the activity recorder, and the task queue.
func newTemporalHarness(t *testing.T, wf interface{}) (*temporal.Client, *sagaActivities, string) {
	t.Helper()
	tq := "pulse-it-" + uuid.NewString()
	logger := log.Nop()

	cfg := temporal.DefaultConfig()
	cfg.Enabled = true
	cfg.Connection.HostPort = temporalHostPort()
	cfg.Connection.Namespace = "default"
	cfg.Worker.TaskQueue = tq

	tcli, err := temporal.NewClient(cfg, temporal.Deps{Logger: logger})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tworker, err := temporal.NewWorker(cfg, temporal.Deps{Logger: logger, SDKClient: tcli.SDK()})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	acts := &sagaActivities{}
	tworker.RegisterWorkflow(wf)
	tworker.RegisterActivity(acts)

	ctx := context.Background()
	if err := tcli.Start(ctx); err != nil {
		t.Fatalf("client.Start (is Temporal reachable at %s?): %v", temporalHostPort(), err)
	}
	t.Cleanup(func() { _ = tcli.Stop(context.Background()) })
	if err := tworker.Start(ctx); err != nil {
		t.Fatalf("worker.Start: %v", err)
	}
	t.Cleanup(func() { _ = tworker.Stop(context.Background()) })

	return tcli, acts, tq
}

func TestIntegration_SagaHappyPath(t *testing.T) {
	tcli, acts, tq := newTemporalHarness(t, happySagaWorkflow)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, happySagaWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("workflow should succeed, got: %v", err)
	}

	if got := acts.snapshot(); !reflect.DeepEqual(got, []string{"debit", "credit"}) {
		t.Fatalf("activity order = %v, want [debit credit] (no compensation)", got)
	}
}

func TestIntegration_SagaCompensates(t *testing.T) {
	tcli, acts, tq := newTemporalHarness(t, compensateSagaWorkflow)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue:                tq,
		WorkflowExecutionTimeout: 30 * time.Second,
	}, compensateSagaWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	// The workflow must fail (the failing step propagates after compensation).
	if err := run.Get(ctx, nil); err == nil {
		t.Fatal("workflow should fail after the second step errors")
	}

	// debit ran, then the compensation undid it; credit never succeeded.
	if got := acts.snapshot(); !reflect.DeepEqual(got, []string{"debit", "undo-debit"}) {
		t.Fatalf("activity order = %v, want [debit undo-debit] (compensation ran)", got)
	}
}
