//go:build integration

package temporal_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkclient "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/temporal"
	"github.com/tuanlao/pulse/pkg/temporal/saga"
)

// temporalCW returns an (unstarted) client + worker on a fresh task queue, so the
// test can register its own workflows/activities before starting.
func temporalCW(t *testing.T) (*temporal.Client, *temporal.Worker, string) {
	t.Helper()
	tq := "pulse-it-wf-" + uuid.NewString()
	cfg := temporal.DefaultConfig()
	cfg.Enabled = true
	cfg.Connection.HostPort = temporalHostPort()
	cfg.Connection.Namespace = "default"
	cfg.Worker.TaskQueue = tq
	tcli, err := temporal.NewClient(cfg, temporal.Deps{Logger: log.Nop()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	tworker, err := temporal.NewWorker(cfg, temporal.Deps{Logger: log.Nop(), SDKClient: tcli.SDK()})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	return tcli, tworker, tq
}

func startCW(t *testing.T, tcli *temporal.Client, tworker *temporal.Worker) {
	t.Helper()
	if err := tcli.Start(context.Background()); err != nil {
		t.Fatalf("client.Start (Temporal at %s?): %v", temporalHostPort(), err)
	}
	t.Cleanup(func() { _ = tcli.Stop(context.Background()) })
	if err := tworker.Start(context.Background()); err != nil {
		t.Fatalf("worker.Start: %v", err)
	}
	t.Cleanup(func() { _ = tworker.Stop(context.Background()) })
}

// ---- Continue-As-New ----

type canActivities struct{ count atomic.Int32 }

func (a *canActivities) Bump(context.Context) error { a.count.Add(1); return nil }

type canInput struct {
	Remaining  int
	Processed  int
	Thresholds saga.Thresholds
}

func canWorkflow(ctx workflow.Context, in canInput) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second})
	for in.Remaining > 0 {
		if err := workflow.ExecuteActivity(ctx, "Bump").Get(ctx, nil); err != nil {
			return in.Processed, err
		}
		in.Remaining--
		in.Processed++
		if in.Remaining > 0 && saga.ShouldContinueAsNew(ctx, in.Thresholds) {
			return 0, workflow.NewContinueAsNewError(ctx, canWorkflow, in)
		}
	}
	return in.Processed, nil
}

func TestIntegration_ContinueAsNew_ProcessesAllAcrossRuns(t *testing.T) {
	tcli, tworker, tq := temporalCW(t)
	acts := &canActivities{}
	tworker.RegisterWorkflow(canWorkflow)
	tworker.RegisterActivity(acts)
	startCW(t, tcli, tworker)

	const total = 20
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 45 * time.Second,
	}, canWorkflow, canInput{
		Remaining:  total,
		Thresholds: saga.Thresholds{MaxEvents: 5, MaxBytes: 1 << 30, IgnoreServerSuggestion: true},
	})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	var processed int
	if err := run.Get(ctx, &processed); err != nil {
		t.Fatalf("workflow failed (CAN should carry state and complete): %v", err)
	}
	if processed != total {
		t.Fatalf("processed = %d, want %d (state must carry across Continue-As-New)", processed, total)
	}
	if got := acts.count.Load(); int(got) != total {
		t.Fatalf("Bump ran %d times, want %d", got, total)
	}
}

// ---- Signals ----

func signalWorkflow(ctx workflow.Context) (string, error) {
	var sig string
	workflow.GetSignalChannel(ctx, "go").Receive(ctx, &sig)
	return "got:" + sig, nil
}

func TestIntegration_Signal_UnblocksWorkflow(t *testing.T) {
	tcli, tworker, tq := temporalCW(t)
	tworker.RegisterWorkflow(signalWorkflow)
	startCW(t, tcli, tworker)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, signalWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := tcli.SDK().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "go", "hello"); err != nil {
		t.Fatalf("SignalWorkflow: %v", err)
	}
	var out string
	if err := run.Get(ctx, &out); err != nil {
		t.Fatalf("run.Get: %v", err)
	}
	if out != "got:hello" {
		t.Fatalf("result = %q, want got:hello", out)
	}
}

// ---- Queries ----

func queryWorkflow(ctx workflow.Context) error {
	status := "running"
	if err := workflow.SetQueryHandler(ctx, "status", func() (string, error) { return status, nil }); err != nil {
		return err
	}
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
	status = "finished"
	return nil
}

func queryStatus(t *testing.T, tcli *temporal.Client, ctx context.Context, wfID string) string {
	t.Helper()
	for i := 0; i < 50; i++ {
		resp, err := tcli.SDK().QueryWorkflow(ctx, wfID, "", "status")
		if err == nil {
			var s string
			if err := resp.Get(&s); err != nil {
				t.Fatalf("decode query: %v", err)
			}
			return s
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("query never succeeded")
	return ""
}

func TestIntegration_Query_RunningAndCompleted(t *testing.T) {
	tcli, tworker, tq := temporalCW(t)
	tworker.RegisterWorkflow(queryWorkflow)
	startCW(t, tcli, tworker)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, queryWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}

	if s := queryStatus(t, tcli, ctx, run.GetID()); s != "running" {
		t.Fatalf("query while running = %q, want running", s)
	}
	if err := tcli.SDK().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", nil); err != nil {
		t.Fatalf("SignalWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("run.Get: %v", err)
	}
	if s := queryStatus(t, tcli, ctx, run.GetID()); s != "finished" {
		t.Fatalf("query after completion = %q, want finished", s)
	}
}

// ---- Activity retry / timeout ----

type retryActivities struct{ attempts atomic.Int32 }

func (a *retryActivities) FlakyThenOK(context.Context) error {
	if a.attempts.Add(1) < 3 {
		return errors.New("flaky")
	}
	return nil
}

func (a *retryActivities) Slow(ctx context.Context) error {
	select {
	case <-time.After(5 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func retryWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{
			InitialInterval:    50 * time.Millisecond,
			BackoffCoefficient: 1.0,
			MaximumAttempts:    5,
		},
	})
	return workflow.ExecuteActivity(ctx, "FlakyThenOK").Get(ctx, nil)
}

func TestIntegration_Activity_RetriesThenSucceeds(t *testing.T) {
	tcli, tworker, tq := temporalCW(t)
	acts := &retryActivities{}
	tworker.RegisterWorkflow(retryWorkflow)
	tworker.RegisterActivity(acts)
	startCW(t, tcli, tworker)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, retryWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		t.Fatalf("workflow should succeed after activity retries: %v", err)
	}
	if got := acts.attempts.Load(); got != 3 {
		t.Fatalf("activity attempts = %d, want 3 (fail twice, succeed on third)", got)
	}
}

func timeoutWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
		RetryPolicy:         &sdktemporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteActivity(ctx, "Slow").Get(ctx, nil)
}

func TestIntegration_Activity_StartToCloseTimeout(t *testing.T) {
	tcli, tworker, tq := temporalCW(t)
	tworker.RegisterWorkflow(timeoutWorkflow)
	tworker.RegisterActivity(&retryActivities{})
	startCW(t, tcli, tworker)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := tcli.StartWorkflow(ctx, sdkclient.StartWorkflowOptions{
		TaskQueue: tq, WorkflowExecutionTimeout: 30 * time.Second,
	}, timeoutWorkflow)
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if err := run.Get(ctx, nil); err == nil {
		t.Fatal("workflow should fail when the activity exceeds StartToCloseTimeout")
	}
}
