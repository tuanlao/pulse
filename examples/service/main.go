// Command service is the canonical composition root demonstrating how to wire
// pulse components together. There is no god aggregator config: the service owns
// its own config struct (appConfig below), embedding the pulse component Configs
// it needs plus its own cross-cutting fields, and loads it with pkg/config.
//
//   - define appConfig + defaults, load it from YAML (defaults < config.yaml)
//   - normalize() propagates Env/ServiceName/Version into sub-configs and derives
//     the gin mode from Env (via pkg/env) — the bit the old app.Normalize did
//   - build logger, tracer, metrics
//   - build the HTTP server, an outbound HTTP client, a redis client and a cron
//     scheduler (all sharing the metrics registry, so /metrics exposes them all)
//   - register components into the lifecycle manager (tracing first, redis and
//     cron in the middle, server last)
//
// The /call/:name route uses the outbound client to call this service's own
// /hello/:name, showing the trace id propagates into headers. The /cache route
// (when redis is enabled) demonstrates rueidis client-side caching.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/config"
	"github.com/tuanlao/pulse/pkg/cron"
	"github.com/tuanlao/pulse/pkg/env"
	grpcclient "github.com/tuanlao/pulse/pkg/grpc/client"
	grpcserver "github.com/tuanlao/pulse/pkg/grpc/server"
	grpcinterceptor "github.com/tuanlao/pulse/pkg/grpc/server/interceptor"
	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/kafka"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
	"github.com/tuanlao/pulse/pkg/redis"
	"github.com/tuanlao/pulse/pkg/snowflake"
	"github.com/tuanlao/pulse/pkg/swagger"
	"github.com/tuanlao/pulse/pkg/temporal"
	"github.com/tuanlao/pulse/pkg/tracing"
	"go.uber.org/zap"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// appConfig is THIS service's config. Pulse has no unified app.Config — a service
// composes the component Configs it needs (here every pulse component) alongside
// its own fields. It is nested-object shaped so it maps onto structured YAML.
type appConfig struct {
	Env         env.Env `mapstructure:"env"`
	ServiceName string  `mapstructure:"service_name"`
	Version     string  `mapstructure:"version"`

	Log       log.Config       `mapstructure:"log"`
	Server    server.Config    `mapstructure:"server"`
	Tracing   tracing.Config   `mapstructure:"tracing"`
	Metrics   metrics.Config   `mapstructure:"metrics"`
	Swagger   swagger.Config   `mapstructure:"swagger"`
	Redis     redis.Config     `mapstructure:"redis"`
	Cron      cron.Config      `mapstructure:"cron"`
	Kafka     kafka.Config     `mapstructure:"kafka"`
	Snowflake snowflake.Config `mapstructure:"snowflake"`
	Temporal  temporal.Config  `mapstructure:"temporal"`

	Grpc       grpcserver.Config `mapstructure:"grpc"`
	GrpcClient grpcclient.Config `mapstructure:"grpc_client"`

	// Named upstream HTTP clients (e.g. "self", "payment").
	HttpClients map[string]client.Config `mapstructure:"http_clients"`

	Lifecycle lifecycle.Config `mapstructure:"lifecycle"`
}

// defaultAppConfig composes each component's defaults. Server.Mode is left empty
// so normalize() can derive it from Env.
func defaultAppConfig() appConfig {
	srv := server.DefaultConfig()
	srv.Mode = "" // let Env drive the gin mode in normalize()

	return appConfig{
		Env:         env.EnvDev,
		ServiceName: "example-service",
		Version:     "dev",
		Log:         log.DefaultConfig(),
		Server:      srv,
		Tracing:     tracing.DefaultConfig(),
		Metrics:     metrics.DefaultConfig(),
		Swagger:     swagger.DefaultConfig(),
		Redis:       redis.DefaultConfig(),
		Cron:        cron.DefaultConfig(),
		Kafka:       kafka.DefaultConfig(),
		Snowflake:   snowflake.DefaultConfig(),
		Temporal:    temporal.DefaultConfig(),
		Grpc:        grpcserver.DefaultConfig(),
		GrpcClient:  grpcclient.DefaultConfig(),
		HttpClients: map[string]client.Config{},
		Lifecycle:   lifecycle.DefaultConfig(),
	}
}

// normalize propagates cross-cutting values into sub-configs and derives the gin
// mode from Env — what the removed app.Normalize used to do, now wired explicitly
// by the service. It is idempotent.
func (c *appConfig) normalize() {
	c.Env = c.Env.Normalize()
	if c.ServiceName != "" && c.Tracing.ServiceName == "" {
		c.Tracing.ServiceName = c.ServiceName
	}
	if c.Version != "" && c.Tracing.ServiceVersion == "" {
		c.Tracing.ServiceVersion = c.Version
	}
	if c.Tracing.Environment == "" {
		c.Tracing.Environment = string(c.Env)
	}
	if c.Server.Mode == "" {
		c.Server.Mode = c.Env.GinMode()
	}
	// Propagate cross-cutting values into the kafka config (used for x-source and
	// the gin-mode-like Env derivations).
	if c.Kafka.ServiceName == "" {
		c.Kafka.ServiceName = c.ServiceName
	}
	if c.Kafka.Version == "" {
		c.Kafka.Version = c.Version
	}
	if c.Kafka.Env == "" {
		c.Kafka.Env = c.Env
	}
	// Default the worker task queue from the service name so a service only needs
	// to flip temporal.enabled on.
	if c.Temporal.Worker.TaskQueue == "" {
		c.Temporal.Worker.TaskQueue = c.ServiceName + "-tasks"
	}
	// Default the gRPC client target to this service's own gRPC server when unset
	// (analogous to the "self" HTTP client's base_url default).
	if c.GrpcClient.Target == "" {
		c.GrpcClient.Target = fmt.Sprintf("127.0.0.1:%d", c.Grpc.Port)
	}
}

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

	// 1. Load this service's config (defaults < config.yaml). YAML-only, then
	// normalize cross-cutting values explicitly.
	cfg := defaultAppConfig()
	if err := config.Load(&cfg, config.DefaultOptions()); err != nil {
		return err
	}
	cfg.normalize()

	// 2. Logger — Sync is the OUTERMOST defer so shutdown logs flush last.
	logger, err := log.New(cfg.Log)
	if err != nil {
		return err
	}
	defer func() { _ = logger.Sync() }()

	// 3. Tracing + metrics. Component metrics register into the server RED registry
	// so a single /metrics endpoint exposes all of them.
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

	// 6. Redis client (rueidis). Disabled by default in config.yaml so the demo
	// runs without a redis server; set redis.enabled: true (and run redis >= 6)
	// to exercise client-side caching via the /cache route. New returns a no-op
	// client when disabled, so registering it is always safe.
	var redisMetrics *redis.Metrics
	if cfg.Redis.Enabled && red != nil {
		if redisMetrics, err = redis.NewMetrics(cfg.Redis.Metrics, red.Registry()); err != nil {
			return err
		}
	}
	rdb, err := redis.New(cfg.Redis, redis.Deps{
		Logger:         logger,
		TracerProvider: tracer.Provider(),
		Metrics:        redisMetrics,
	})
	if err != nil {
		return err
	}

	registerRoutes(srv, selfClient, logger)
	swagger.Mount(srv.Engine(), cfg.Swagger)
	srv.Readiness().Register("self", func(context.Context) error { return nil })

	if cfg.Redis.Enabled {
		// Seed a demo key once; repeated cached GETs are served from the local
		// client-side cache (no round trip) until the key is invalidated.
		if err := rdb.Do(ctx, rdb.B().Set().Key("demo:greeting").Value("hello from redis").Build()).Error(); err != nil {
			return err
		}
		registerCacheRoute(srv, rdb, logger)
		srv.Readiness().Register("redis", rdb.CheckReady)
	}

	// 7. Cron scheduler with a demo heartbeat job. Cron metrics share the same
	// registry, so /metrics exposes server + client + redis + cron metrics together.
	var cronMetrics *cron.CronMetrics
	if red != nil {
		if cronMetrics, err = cron.NewMetrics(cfg.Cron.Metrics, red.Registry()); err != nil {
			return err
		}
	}
	cronDeps := cron.Deps{
		Logger:         logger,
		TracerProvider: tracer.Provider(),
		Metrics:        cronMetrics,
	}
	// Reuse the shared rueidis client for cron's distributed lock (so there's one
	// redis client). The lock only activates when cron.lock.enabled is true.
	if cfg.Redis.Enabled {
		cronDeps.RedisClient = rdb
	}
	cronSched, err := cron.New(cfg.Cron, cronDeps)
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

	// 7b. Snowflake id generator. Default strategy is "static" so the demo runs
	// without redis; set snowflake.worker_id.strategy: redis (and redis.enabled) to
	// have pods contend for a worker-id slot. Metrics share the same registry.
	var snowflakeMetrics *snowflake.Metrics
	if red != nil {
		if snowflakeMetrics, err = snowflake.NewMetrics(cfg.Snowflake.Metrics, red.Registry()); err != nil {
			return err
		}
	}
	snowflakeDeps := snowflake.Deps{
		Logger:         logger,
		TracerProvider: tracer.Provider(),
		Metrics:        snowflakeMetrics,
	}
	// Reuse the shared rueidis client for the redis worker-id strategy.
	if cfg.Redis.Enabled {
		snowflakeDeps.RedisClient = rdb
	}
	gen, err := snowflake.New(cfg.Snowflake, snowflakeDeps)
	if err != nil {
		return err
	}
	registerIDRoute(srv, gen, logger)
	if cfg.Snowflake.Enabled {
		srv.Readiness().Register("snowflake", gen.CheckReady)
	}

	// 8. Kafka producer + consumer. Disabled by default in config.yaml so the demo
	// runs without a broker (New returns no-op components when disabled). The
	// consumer reuses the shared rueidis client for the global (redis) deduper when
	// redis is enabled; otherwise dedup defaults to the in-process (local) cache.
	var kafkaMetrics *kafka.Metrics
	if red != nil {
		if kafkaMetrics, err = kafka.NewMetrics(cfg.Kafka.Metrics, red.Registry()); err != nil {
			return err
		}
	}
	kafkaDeps := kafka.Deps{Logger: logger, TracerProvider: tracer.Provider(), Metrics: kafkaMetrics}
	if cfg.Redis.Enabled {
		kafkaDeps.RedisClient = rdb
	}
	kProducer, err := kafka.NewProducer(cfg.Kafka, kafkaDeps)
	if err != nil {
		return err
	}
	// Build every consumer declared under kafka.consumers in config.yaml. Each is a
	// fully independent consumer group; look one up by its config name (the map key)
	// to bind handlers. For a one-off consumer not in config, declare it in code:
	//   c, _ := kafka.NewConsumer(cfg.Kafka, kafkaDeps,
	//       kafka.WithGroupID("ad-hoc"), kafka.WithTopics("ad-hoc-topic"))
	kConsumers, err := kafka.NewConsumers(cfg.Kafka, kafkaDeps)
	if err != nil {
		return err
	}
	// "orders" consumer: typed handler — the payload is JSON-decoded into orderEvent
	// before it runs. Returning an error exercises the retry-topic pipeline;
	// kafka.NonRetryable would route straight to the DLQ.
	if c, ok := kConsumers.Get("orders"); ok {
		kafka.On(c, "orders", func(hctx context.Context, e orderEvent, _ *kafka.Message) error {
			log.FromContext(hctx, logger).Info("order received", zap.String("id", e.ID), zap.Int("amount", e.Amount))
			return nil
		})
	}
	// "audit" consumer: a SEPARATE group on the same topic, so it receives its own
	// copy of every record (fan-out). Bind its handler the same way.
	if c, ok := kConsumers.Get("audit"); ok {
		kafka.On(c, "orders", func(hctx context.Context, e orderEvent, _ *kafka.Message) error {
			log.FromContext(hctx, logger).Info("order audited", zap.String("id", e.ID))
			return nil
		})
	}
	registerPublishRoute(srv, kProducer, logger)

	// 8b. Temporal (saga orchestrator). Disabled by default in config.yaml so the
	// demo runs without a Temporal server; run `temporal server start-dev` and set
	// temporal.enabled=true to exercise POST /transfer. The client dials once and
	// owns the connection; the worker runs on the SAME connection (shared, not
	// owned). The SDK's own metrics bridge into the shared registry via tally.
	tDeps := temporal.Deps{Logger: logger, TracerProvider: tracer.Provider()}
	if red != nil {
		tDeps.Registry = red.Registry()
	}
	tcli, err := temporal.NewClient(cfg.Temporal, tDeps)
	if err != nil {
		return err
	}
	tworker, err := temporal.NewWorker(cfg.Temporal, temporal.Deps{Logger: logger, SDKClient: tcli.SDK()})
	if err != nil {
		return err
	}
	// Register workflows + activities before the worker starts. These are no-ops
	// when the worker is disabled.
	tworker.RegisterWorkflow(TransferSaga)
	tworker.RegisterWorkflow(OrderSagaWorkflow)
	tworker.RegisterActivity(Debit)
	tworker.RegisterActivity(Credit)
	registerTransferRoute(srv, tcli, cfg.Temporal, logger)
	if cfg.Temporal.Enabled {
		srv.Readiness().Register("temporal", tcli.CheckReady)
	}

	// 8c. gRPC server + client. The server serves the health + reflection services
	// by default (register your generated stubs via grpcSrv.Register before Start);
	// the client dials this service's own gRPC server lazily. Both share the metrics
	// registry, so /metrics exposes grpc_server/grpc_client series too. The
	// /grpc/health route below calls the server through the client end-to-end.
	var grpcServerMetrics *grpcinterceptor.Metrics
	var grpcClientMetrics *grpcclient.ClientMetrics
	if red != nil {
		if grpcServerMetrics, err = grpcinterceptor.NewMetrics(cfg.Grpc.Metrics, red.Registry()); err != nil {
			return err
		}
		if cfg.GrpcClient.Metrics.Enabled {
			if grpcClientMetrics, err = grpcclient.NewMetrics(cfg.GrpcClient.Metrics, red.Registry()); err != nil {
				return err
			}
		}
	}
	grpcSrv, err := grpcserver.New(cfg.Grpc, grpcserver.Deps{
		Logger:         logger,
		Metrics:        grpcServerMetrics,
		TracerProvider: tracer.Provider(),
		ServiceName:    cfg.ServiceName,
		OnServeError:   func(error) { cancel() },
	})
	if err != nil {
		return err
	}
	// Register your generated stubs here before Start, e.g.:
	//   grpcSrv.Register(func(s *grpc.Server) { pb.RegisterMyServiceServer(s, impl) })
	srv.Readiness().Register("grpc", grpcSrv.CheckReady)

	grpcClient, err := grpcclient.New(cfg.GrpcClient, grpcclient.Deps{
		Logger:         logger,
		Metrics:        grpcClientMetrics,
		TracerProvider: tracer.Provider(),
	})
	if err != nil {
		return err
	}
	registerGrpcHealthRoute(srv, grpcClient, logger)

	// 9. Lifecycle: tracing first, server last → server drains before tracing
	// flushes on shutdown. Redis, kafka and cron sit between (stop after the server).
	mgr := lifecycle.New(cfg.Lifecycle, logger.LifecycleAdapter())
	mgr.Register(tracer)
	if cfg.Redis.Enabled {
		mgr.Register(rdb)
	}
	mgr.Register(kProducer)
	mgr.Register(kConsumers.Components()...) // every declared consumer (each its own group)
	mgr.Register(cronSched)
	mgr.Register(gen)     // after redis (stops before redis closes), before the server
	mgr.Register(tcli)    // temporal client owns the connection
	mgr.Register(tworker) // registered after the client so it drains first, then the client closes the connection
	mgr.Register(grpcClient)
	mgr.Register(grpcSrv) // gRPC server drains before its dependencies (registered just before the HTTP server)
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

// registerIDRoute mints a snowflake id and returns it with its decoded parts. It
// uses TryGenerate so the route responds with an error (instead of panicking) if
// the generator is disabled or — for the redis strategy — not yet ready/fenced.
func registerIDRoute(srv *server.Server, gen *snowflake.Generator, base *log.Logger) {
	srv.Engine().GET("/id", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		id, err := gen.TryGenerate()
		if err != nil {
			l.Error("snowflake generate failed", zap.Error(err))
			gc.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		gc.JSON(http.StatusOK, gin.H{
			"id":     id.String(),
			"base58": id.Base58(),
			"node":   gen.Node(id),
			"step":   gen.Step(id),
			"time":   gen.TimeAt(id).UTC().Format(time.RFC3339Nano),
		})
	})
}

// registerGrpcHealthRoute calls this service's own gRPC server health Check
// through the gRPC client, demonstrating the outbound client end-to-end (the
// inbound request's trace id propagates into the gRPC metadata).
func registerGrpcHealthRoute(srv *server.Server, gc *grpcclient.Client, base *log.Logger) {
	srv.Engine().GET("/grpc/health", func(gctx *gin.Context) {
		l := log.FromContext(gctx.Request.Context(), base)
		resp, err := grpc_health_v1.NewHealthClient(gc.Conn()).Check(
			gctx.Request.Context(), &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			l.Error("grpc health check failed", zap.Error(err))
			gctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		gctx.JSON(http.StatusOK, gin.H{"status": resp.Status.String()})
	})
}

// orderEvent is the demo payload produced to / consumed from the "orders" topic.
type orderEvent struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

// registerPublishRoute publishes an order event to the "orders" topic via the
// typed Send. When kafka is disabled the producer is a no-op, so the route still
// responds (the message is simply dropped).
func registerPublishRoute(srv *server.Server, p *kafka.Producer, base *log.Logger) {
	srv.Engine().POST("/publish/:id", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		id := gc.Param("id")
		evt := orderEvent{ID: id, Amount: 100}
		if err := kafka.Send(gc.Request.Context(), p, "orders", id, evt); err != nil {
			l.Error("kafka publish failed", zap.Error(err))
			gc.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		gc.JSON(http.StatusOK, gin.H{"published": evt})
	})
}

// registerCacheRoute serves a client-side cached read. The first call is a cache
// miss (round trip); subsequent calls within the TTL are served locally and
// report cache_hit: true.
func registerCacheRoute(srv *server.Server, rdb *redis.Client, base *log.Logger) {
	srv.Engine().GET("/cache", func(gc *gin.Context) {
		l := log.FromContext(gc.Request.Context(), base)
		resp := rdb.DoCache(gc.Request.Context(), rdb.B().Get().Key("demo:greeting").Cache(), time.Minute)
		val, err := resp.ToString()
		if err != nil {
			l.Error("redis cache read failed", zap.Error(err))
			gc.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
			return
		}
		l.Info("cache read", zap.Bool("cache_hit", resp.IsCacheHit()))
		gc.JSON(http.StatusOK, gin.H{"value": val, "cache_hit": resp.IsCacheHit()})
	})
}
