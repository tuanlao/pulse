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
	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
	"github.com/tuanlao/pulse/pkg/redis"
	"github.com/tuanlao/pulse/pkg/swagger"
	"github.com/tuanlao/pulse/pkg/tracing"
	"go.uber.org/zap"
)

// appConfig is THIS service's config. Pulse has no unified app.Config — a service
// composes the component Configs it needs (here every pulse component) alongside
// its own fields. It is nested-object shaped so it maps onto structured YAML.
type appConfig struct {
	Env         env.Env `mapstructure:"env"`
	ServiceName string  `mapstructure:"service_name"`
	Version     string  `mapstructure:"version"`

	Log     log.Config     `mapstructure:"log"`
	Server  server.Config  `mapstructure:"server"`
	Tracing tracing.Config `mapstructure:"tracing"`
	Metrics metrics.Config `mapstructure:"metrics"`
	Swagger swagger.Config `mapstructure:"swagger"`
	Redis   redis.Config   `mapstructure:"redis"`
	Cron    cron.Config    `mapstructure:"cron"`

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

	// 8. Lifecycle: tracing first, server last → server drains before tracing
	// flushes on shutdown. Redis and cron sit between (stop after the server).
	mgr := lifecycle.New(cfg.Lifecycle, logger.LifecycleAdapter())
	mgr.Register(tracer)
	if cfg.Redis.Enabled {
		mgr.Register(rdb)
	}
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
