package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/temporal"
	"github.com/tuanlao/pulse/pkg/temporal/saga"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/zap"
)

// transferInput is the POST /transfer payload and the TransferSaga input.
type transferInput struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

// Debit and Credit are demo activities. A real implementation would call a ledger
// service; here they only log so the demo runs without external dependencies.
func Debit(ctx context.Context, account string, amount int) error {
	activity.GetLogger(ctx).Info("debit", "account", account, "amount", amount)
	return nil
}

func Credit(ctx context.Context, account string, amount int) error {
	activity.GetLogger(ctx).Info("credit", "account", account, "amount", amount)
	return nil
}

// TransferSaga moves money by debiting the source then crediting the destination.
// It registers a compensation after each successful step; if a later step fails
// the saga runs the compensations in reverse (LIFO) to undo what completed.
func TransferSaga(ctx workflow.Context, in transferInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	s := saga.New(ctx)
	return s.Run(ctx, func() error {
		if err := workflow.ExecuteActivity(ctx, Debit, in.From, in.Amount).Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation(Credit, in.From, in.Amount) // undo the debit
		if err := workflow.ExecuteActivity(ctx, Credit, in.To, in.Amount).Get(ctx, nil); err != nil {
			return err
		}
		s.AddActivityCompensation(Debit, in.To, in.Amount) // undo the credit
		return nil
	})
}

// orderBatchInput drives the Continue-As-New demo. Remaining items are processed
// one at a time; before the per-run history can balloon the workflow Continues-As-
// New with the carried-forward state, keeping each run's history (and worker
// memory) bounded. Thresholds come from config (deploy-time constant, hence
// deterministic across replays).
type orderBatchInput struct {
	Remaining  int             `json:"remaining"`
	Thresholds saga.Thresholds `json:"thresholds"`
}

// OrderSagaWorkflow processes a long batch, Continuing-As-New to bound history.
func OrderSagaWorkflow(ctx workflow.Context, in orderBatchInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	})
	for in.Remaining > 0 {
		if err := workflow.ExecuteActivity(ctx, Debit, "batch", 1).Get(ctx, nil); err != nil {
			return err
		}
		in.Remaining--
		if in.Remaining > 0 && saga.ShouldContinueAsNew(ctx, in.Thresholds) {
			// Carry the remaining work forward into a fresh history.
			return workflow.NewContinueAsNewError(ctx, OrderSagaWorkflow, in)
		}
	}
	return nil
}

// registerTransferRoute exposes POST /transfer, which starts the TransferSaga.
// When temporal is disabled the client is a no-op and StartWorkflow returns an
// error, so the route reports 503.
func registerTransferRoute(srv *server.Server, tcli *temporal.Client, cfg temporal.Config, base *log.Logger) {
	srv.Engine().POST("/transfer", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		var in transferInput
		if err := gc.ShouldBindJSON(&in); err != nil {
			gc.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		run, err := tcli.StartWorkflow(gc.Request.Context(), sdkclient.StartWorkflowOptions{
			TaskQueue: cfg.Worker.TaskQueue,
		}, TransferSaga, in)
		if err != nil {
			l.Error("start transfer saga failed", zap.Error(err))
			gc.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		gc.JSON(http.StatusOK, gin.H{"workflow_id": run.GetID(), "run_id": run.GetRunID()})
	})
}
