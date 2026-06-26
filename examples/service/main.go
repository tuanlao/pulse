// Command service is the canonical composition root demonstrating how to wire
// pulse components together using the unified app.Config:
//
//   - load app.Config (env + all component configs) from YAML
//   - build logger, tracer, metrics
//   - build the HTTP server and an outbound HTTP client (sharing the metrics
//     registry so /metrics exposes both inbound RED and outbound client metrics)
//   - register components into the lifecycle manager (tracing first, server last)
//
// The /call/:name route uses the outbound client to call this service's own
// /hello/:name, showing that the trace id propagates from the inbound request
// through the client into headers.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/app"
	"github.com/tuanlao/pulse/pkg/config"
	"github.com/tuanlao/pulse/pkg/cron"
	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
	"github.com/tuanlao/pulse/pkg/swagger"
	"github.com/tuanlao/pulse/pkg/tracing"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	// A cancelable context lets a fatal background failure (e.g. the HTTP server's
	// Serve goroutine dying) unblock mgr.Run and trigger an ordered shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Load unified app config (defaults < config.yaml). Config is YAML-only.
	cfg := app.DefaultConfig()
	cfg.ServiceName = "example-service"
	// Tracing is off by default so the demo runs without a collector; the client
	// still propagates a generated trace id. Set tracing.enabled: true and
	// tracing.endpoint in config.yaml to export to a collector.
	cfg.Tracing.Enabled = false
	if err := app.Load(&cfg, config.DefaultOptions()); err != nil {
		return err
	}

	// 2. Logger — Sync is the OUTERMOST defer so shutdown logs flush last.
	logger, err := log.New(cfg.Log)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	// 3. Tracing + metrics. Client metrics register into the server RED registry
	// so a single /metrics endpoint exposes both.
	tracer, err := tracing.New(ctx, cfg.Tracing)
	if err != nil {
		return err
	}

	// Outbound client config comes from cfg.HttpClients (this demo uses the "self"
	// entry, declared in config.yaml). client.New backfills any unset field from
	// DefaultConfig, so a partial YAML entry is fine.
	selfCfg, ok := cfg.HttpClients["self"]
	if !ok {
		selfCfg = client.DefaultConfig()
	}
	if selfCfg.BaseURL == "" {
		selfCfg.BaseURL = fmt.Sprintf("http://127.0.0.1:%d", cfg.Server.Port)
	}

	var red *metrics.RED
	var clientMetrics *client.ClientMetrics
	if cfg.Metrics.Enabled {
		if red, err = metrics.New(cfg.Metrics); err != nil {
			return err
		}
		if clientMetrics, err = client.NewMetrics(selfCfg.Metrics, red.Registry()); err != nil {
			return err
		}
	}

	// 4. HTTP server. OnServeError cancels the root context so a fatal serve
	// failure unblocks mgr.Run and shuts the service down in order.
	srv, err := server.New(cfg.Server, server.Deps{
		Logger:         logger,
		Metrics:        red,
		TracerProvider: tracer.Provider(),
		ServiceName:    cfg.ServiceName,
		OnServeError:   func(error) { cancel() },
	})
	if err != nil {
		return err
	}

	// 5. Outbound client pointing at this service (demo).
	selfClient, err := client.New(selfCfg, client.Deps{
		Logger:         logger,
		Metrics:        clientMetrics,
		TracerProvider: tracer.Provider(),
	})
	if err != nil {
		return err
	}

	registerRoutes(srv, selfClient, logger)
	swagger.Mount(srv.Engine(), cfg.Swagger)
	srv.Readiness().Register("self", func(context.Context) error { return nil })

	// 6. Cron scheduler with a demo heartbeat job. Cron metrics share the same
	// registry, so /metrics exposes server + client + cron metrics together.
	var cronMetrics *cron.CronMetrics
	if red != nil {
		if cronMetrics, err = cron.NewMetrics(cfg.Cron.Metrics, red.Registry()); err != nil {
			return err
		}
	}
	cronSched, err := cron.New(cfg.Cron, cron.Deps{
		Logger:         logger,
		TracerProvider: tracer.Provider(),
		Metrics:        cronMetrics,
	})
	if err != nil {
		return err
	}
	// Register the handler by name; the schedule is declared in config
	// (cron.jobs.heartbeat) and wired automatically when the scheduler starts.
	cronSched.Register("heartbeat", func(jobCtx context.Context) error {
		// The job logger already carries trace_id/span_id from the job context.
		log.FromContext(jobCtx, logger).Info("heartbeat tick")
		return nil
	})

	// 7. Lifecycle: tracing first, server last → server drains before tracing
	// flushes on shutdown. Cron sits between (stops after the server).
	mgr := lifecycle.New(cfg.Lifecycle, logger.LifecycleAdapter())
	mgr.Register(tracer)
	mgr.Register(cronSched)
	mgr.Register(srv)

	logger.Info("starting service",
		zap.String("env", string(cfg.Env)),
		zap.Int("port", cfg.Server.Port))
	return mgr.Run(ctx)
}

func registerRoutes(srv *server.Server, c *client.Client, base *log.Logger) {
	srv.Engine().GET("/hello/:name", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		name := gc.Param("name")
		l.Info("handling hello", zap.String("name", name))
		gc.JSON(http.StatusOK, gin.H{
			"message": "hello " + name,
			"at":      time.Now().UTC().Format(time.RFC3339),
		})
	})

	// /call/:name uses the outbound client to call /hello/:name on this service.
	// The inbound request's trace id flows into the outbound call's headers.
	srv.Engine().GET("/call/:name", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		name := gc.Param("name")
		var out map[string]any
		if err := c.GetJSON(gc.Request.Context(), "/hello/"+name, &out); err != nil {
			l.Error("outbound call failed", zap.Error(err))
			gc.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		gc.JSON(http.StatusOK, gin.H{"upstream": out})
	})
}
