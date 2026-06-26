package cron

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/tuanlao/pulse/pkg/log"
	pulseredis "github.com/tuanlao/pulse/pkg/redis"
	"github.com/tuanlao/pulse/pkg/tracing"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// JobFunc is a scheduled job. It receives a context that carries a trace id /
// span id and the request-scoped logger, and is cancelled on shutdown (or when
// the per-job timeout fires). Returning an error marks the run as failed.
type JobFunc func(ctx context.Context) error

// Deps are optional collaborators. Nil collaborators degrade gracefully.
type Deps struct {
	// Logger logs job lifecycle; nil falls back to a no-op logger.
	Logger *log.Logger
	// TracerProvider creates a span per job run; nil uses a no-op provider.
	TracerProvider trace.TracerProvider
	// Metrics enables per-job Prometheus metrics; nil disables them.
	Metrics *CronMetrics
	// Locker overrides the distributed locker (highest precedence; bypasses
	// Config.Lock entirely).
	Locker gocron.Locker
	// RedisClient, when set and Config.Lock.Enabled, builds the distributed locker
	// from this shared rueidis client instead of constructing one from
	// Config.Lock.Redis. The shared client is not owned (not closed on Stop).
	RedisClient rueidis.Client
}

// Scheduler wraps a gocron scheduler and implements lifecycle.Component.
type Scheduler struct {
	cfg         Config
	deps        Deps
	sched       gocron.Scheduler
	tracer      trace.Tracer
	baseCtx     context.Context
	ownedRedis  rueidis.Client     // closed on Stop only when built by this package
	mu          sync.RWMutex       // guards handlers and jobTimeouts
	handlers    map[string]JobFunc // name -> handler, for jobs declared in config
	jobTimeouts map[string]time.Duration
	started     bool
}

// New builds a Scheduler: it resolves the timezone, optionally builds the redis
// distributed locker, and constructs the gocron scheduler with a logging adapter.
func New(cfg Config, deps Deps, opts ...Option) (*Scheduler, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}
	tp := deps.TracerProvider
	if tp == nil {
		tp = noop.NewTracerProvider()
	}

	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, fmt.Errorf("cron: invalid timezone %q: %w", cfg.Timezone, err)
	}

	locker, owned, err := buildLocker(cfg, deps)
	if err != nil {
		return nil, err
	}

	schedOpts := []gocron.SchedulerOption{
		gocron.WithLocation(loc),
		gocron.WithLogger(deps.Logger.GocronAdapter()),
		gocron.WithStopTimeout(cfg.StopTimeout),
	}
	if locker != nil {
		schedOpts = append(schedOpts, gocron.WithDistributedLocker(locker))
	}

	sched, err := gocron.NewScheduler(schedOpts...)
	if err != nil {
		if owned != nil {
			owned.Close()
		}
		return nil, fmt.Errorf("cron: new scheduler: %w", err)
	}

	return &Scheduler{
		cfg:         cfg,
		deps:        deps,
		sched:       sched,
		tracer:      tp.Tracer("github.com/tuanlao/pulse/pkg/cron"),
		baseCtx:     context.Background(),
		ownedRedis:  owned,
		handlers:    make(map[string]JobFunc),
		jobTimeouts: make(map[string]time.Duration),
	}, nil
}

// buildLocker returns the gocron distributed locker (or nil when no lock is
// configured) plus the rueidis client this package owns (to close on Stop). With
// a per-job lock, each scheduled run is taken by whichever pod wins the lock, so
// load is distributed across pods.
//
// Precedence: Deps.Locker (explicit) > Config.Lock.Enabled. When the lock is
// enabled, the rueidis client is Deps.RedisClient (shared, not owned) or one
// built from Config.Lock.Redis (owned, closed on Stop).
func buildLocker(cfg Config, deps Deps) (gocron.Locker, rueidis.Client, error) {
	if deps.Locker != nil {
		return deps.Locker, nil, nil
	}
	if !cfg.Lock.Enabled {
		return nil, nil, nil // no distributed lock
	}

	client := deps.RedisClient
	if rc, ok := client.(*pulseredis.Client); ok && !rc.Enabled() {
		// A disabled *pulseredis.Client is a non-nil interface wrapping a nil
		// rueidis.Client (it never dialed); using it would panic on first Lock.
		// Treat it as "no shared client" and build a dedicated lock client below.
		client = nil
	}
	var owned rueidis.Client
	if client == nil {
		c, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  []string{cfg.Lock.Redis.Address},
			Username:     cfg.Lock.Redis.Username,
			Password:     cfg.Lock.Redis.Password,
			SelectDB:     cfg.Lock.Redis.DB,
			DisableCache: true, // a lock client needs no client-side caching
		})
		if err != nil {
			return nil, nil, fmt.Errorf("cron: build redis lock client: %w", err)
		}
		client = c
		owned = c
	}

	locker := pulseredis.NewLocker(client,
		pulseredis.WithLockKeyPrefix(cfg.Lock.KeyPrefix),
		pulseredis.WithLockTries(cfg.Lock.Tries),
		pulseredis.WithLockTTL(cfg.Lock.TTL),
		pulseredis.WithLockRetryDelay(cfg.Lock.RetryDelay),
	)
	return gocronLocker{locker}, owned, nil
}

// gocronLocker adapts a *pulseredis.Locker to gocron.Locker. A failed
// acquisition (ErrLockNotAcquired) is returned as an error so gocron skips the
// run — exactly the "one pod per scheduled run" behavior.
type gocronLocker struct{ l *pulseredis.Locker }

func (g gocronLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	lk, err := g.l.Lock(ctx, key)
	if err != nil {
		return nil, err
	}
	return gocronLock{lk}, nil
}

// gocronLock adapts a *pulseredis.Lock to gocron.Lock.
type gocronLock struct{ lk *pulseredis.Lock }

func (g gocronLock) Unlock(ctx context.Context) error { return g.lk.Unlock(ctx) }

// Register binds a handler to a job name so the job can be declared (scheduled)
// in configuration (Config.Jobs). Call it before the scheduler is started.
func (s *Scheduler) Register(name string, fn JobFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[name] = fn
}

// AddJob schedules fn under the given definition and name. Extra gocron job
// options (e.g. gocron.WithEventListeners, gocron.WithLimitedRuns) may be passed.
// It returns the job id.
func (s *Scheduler) AddJob(def gocron.JobDefinition, name string, fn JobFunc, opts ...gocron.JobOption) (uuid.UUID, error) {
	task := gocron.NewTask(func(ctx context.Context) { s.runJob(ctx, name, fn) })

	jobOpts := []gocron.JobOption{
		gocron.WithName(name),
		gocron.WithContext(s.baseCtx),
	}
	if s.cfg.Singleton {
		jobOpts = append(jobOpts, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	jobOpts = append(jobOpts, opts...)

	job, err := s.sched.NewJob(def, task, jobOpts...)
	if err != nil {
		return uuid.Nil, err
	}
	return job.ID(), nil
}

// runJob is the wrapper applied to every job: it sets up a span + request-scoped
// logger (generating a trace id when none exists), applies the optional per-job
// timeout, recovers panics, and records metrics.
func (s *Scheduler) runJob(parent context.Context, name string, fn JobFunc) {
	start := time.Now()
	ctx := parent
	if ctx == nil {
		ctx = context.Background()
	}
	// Per-job timeout overrides the global one.
	timeout := s.cfg.JobTimeout
	s.mu.RLock()
	t, ok := s.jobTimeouts[name]
	s.mu.RUnlock()
	if ok && t > 0 {
		timeout = t
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ctx, span := s.tracer.Start(ctx, "cron."+name)
	if !span.SpanContext().IsValid() {
		// Tracing disabled: synthesize a trace id so logs/propagation have one.
		ctx = tracing.WithGeneratedSpanContext(ctx)
	}

	l := s.deps.Logger.ForContext(ctx)
	ctx = log.IntoContext(ctx, l)

	if s.deps.Metrics != nil {
		s.deps.Metrics.start()
	}

	status := "success"
	defer func() {
		if r := recover(); r != nil {
			status = "panic"
			l.Error("cron job panicked",
				zap.String("job", name),
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
			)
			span.RecordError(fmt.Errorf("panic: %v", r))
			span.SetStatus(codes.Error, "panic")
		}
		dur := time.Since(start)
		if s.deps.Metrics != nil {
			s.deps.Metrics.finish(name, status, dur)
		}
		span.End()
	}()

	l.Info("cron job start", zap.String("job", name))
	if err := fn(ctx); err != nil {
		status = "error"
		l.Error("cron job failed",
			zap.String("job", name),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	l.Info("cron job done", zap.String("job", name), zap.Duration("duration", time.Since(start)))
}

// Scheduler returns the underlying gocron scheduler (escape hatch).
func (s *Scheduler) Scheduler() gocron.Scheduler { return s.sched }

// Config returns the resolved configuration.
func (s *Scheduler) Config() Config { return s.cfg }

// Name implements lifecycle.Component.
func (s *Scheduler) Name() string { return "cron" }

// Start schedules the jobs declared in config (binding them to handlers
// registered via Register) and begins scheduling. It is non-blocking. When
// disabled it is a no-op. A config job with no registered handler is a fatal
// error (fail-fast).
func (s *Scheduler) Start(context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}
	if err := s.scheduleConfigured(); err != nil {
		return err
	}
	s.sched.Start()
	s.started = true
	return nil
}

// scheduleConfigured wires every enabled Config.Jobs entry to its registered
// handler.
func (s *Scheduler) scheduleConfigured() error {
	scheduled := make(map[string]struct{}, len(s.cfg.Jobs))
	for name, jc := range s.cfg.Jobs {
		if !jc.Enabled {
			continue
		}
		s.mu.RLock()
		fn, ok := s.handlers[name]
		s.mu.RUnlock()
		if !ok {
			return fmt.Errorf("cron: no handler registered for job %q (call Register before Start)", name)
		}
		def, err := jobDefinition(jc)
		if err != nil {
			return fmt.Errorf("cron: job %q: %w", name, err)
		}
		if jc.Timeout > 0 {
			s.mu.Lock()
			s.jobTimeouts[name] = jc.Timeout
			s.mu.Unlock()
		}
		if _, err := s.AddJob(def, name, fn); err != nil {
			return fmt.Errorf("cron: schedule job %q: %w", name, err)
		}
		scheduled[name] = struct{}{}
	}
	// A handler registered in code but not scheduled by any enabled config job
	// will never run — usually a config typo or a disabled job. Warn so it is not
	// silently dropped.
	s.mu.RLock()
	for name := range s.handlers {
		if _, ok := scheduled[name]; !ok {
			s.deps.Logger.Warn("cron: registered handler has no enabled config job; it will never run",
				zap.String("job", name))
		}
	}
	s.mu.RUnlock()
	return nil
}

// jobDefinition resolves a JobConfig into a gocron job definition.
func jobDefinition(jc JobConfig) (gocron.JobDefinition, error) {
	switch {
	case jc.Cron != "":
		return gocron.CronJob(jc.Cron, jc.WithSeconds), nil
	case jc.Every > 0:
		return gocron.DurationJob(jc.Every), nil
	default:
		return nil, fmt.Errorf("must set either 'cron' or 'every'")
	}
}

// Stop gracefully shuts the scheduler down (waiting for running jobs within
// ctx's deadline) and closes the redis client this package owns.
func (s *Scheduler) Stop(ctx context.Context) error {
	var errs []error
	if s.started {
		if err := s.sched.ShutdownWithContext(ctx); err != nil {
			errs = append(errs, fmt.Errorf("cron: scheduler shutdown: %w", err))
		}
	}
	if s.ownedRedis != nil {
		s.ownedRedis.Close() // rueidis Close is synchronous and returns no error
	}
	return errors.Join(errs...)
}

// Every returns a duration-based job definition (runs every d).
func Every(d time.Duration) gocron.JobDefinition { return gocron.DurationJob(d) }

// Cron returns a crontab-based job definition. withSeconds enables the 6-field
// (seconds) format.
func Cron(spec string, withSeconds bool) gocron.JobDefinition {
	return gocron.CronJob(spec, withSeconds)
}
