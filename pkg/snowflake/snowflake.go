// Package snowflake generates Twitter-style 64-bit Snowflake ids and the rich
// conversion helpers around them (String, Base2/32/36/58/64, bytes, JSON, the
// Parse* family). The layout is the canonical 1 sign + 41 timestamp-ms + NodeBits
// worker + StepBits sequence, with a custom epoch — all per-instance configurable
// (unlike bwmarrin/snowflake's process-global vars).
//
// The worker (node) id is acquired by one of three strategies, selected in
// config:
//
//   - static: a fixed id from config (default; ideal for local/dev/test).
//   - statefulset: the pod ordinal parsed from the pod name ("web-3" -> 3); it
//     errors for a Deployment, whose pod name has a random suffix with no number.
//   - redis: pods contend for a unique slot in [0, pool size) via a redis lease,
//     renewed in the background; if the lease is lost the generator fences
//     (refuses to generate) and re-acquires a slot, so it never knowingly mints
//     ids with a worker id another pod may hold.
//
// Generator implements lifecycle.Component (+ ReadinessChecker). Like every pulse
// package it exposes Config + DefaultConfig() + functional Options.
package snowflake

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/rueidis"
	"github.com/tuanlao/pulse/pkg/log"
	pulseredis "github.com/tuanlao/pulse/pkg/redis"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
)

// Generate / lifecycle errors.
var (
	// ErrDisabled is returned by TryGenerate on a disabled generator.
	ErrDisabled = errors.New("snowflake: generator is disabled")
	// ErrNotReady is returned by TryGenerate before Start has resolved the worker
	// id (the redis strategy resolves its slot in Start).
	ErrNotReady = errors.New("snowflake: generator not started (call Start before generating)")
	// ErrLeaseLost is returned by TryGenerate while the generator is fenced after
	// losing its redis worker-id lease.
	ErrLeaseLost = errors.New("snowflake: generator fenced — redis worker-id lease lost")
	// ErrClockBackwards is returned by TryGenerate when the wall clock moved
	// backwards by more than MaxClockDriftWait.
	ErrClockBackwards = errors.New("snowflake: clock moved backwards beyond max_clock_drift_wait")
)

// Deps are optional collaborators. Nil collaborators degrade gracefully.
type Deps struct {
	// Logger logs lifecycle and lease events; nil falls back to a no-op logger.
	Logger *log.Logger
	// TracerProvider spans the redis worker-id acquisition; nil uses a no-op.
	TracerProvider trace.TracerProvider
	// Metrics enables the package-owned Prometheus metrics; nil disables them.
	Metrics *Metrics
	// RedisClient is a shared rueidis client for the redis strategy (not owned —
	// the caller closes it). When nil/disabled the generator builds and owns a
	// dedicated client from Config.WorkerID.Redis.Redis.
	RedisClient rueidis.Client
}

// Generator mints snowflake ids. It is safe for concurrent use.
type Generator struct {
	cfg     Config
	deps    Deps
	tracer  trace.Tracer
	nowFunc func() time.Time

	// derived bit layout
	epoch     int64
	nodeShift uint8
	timeShift uint8
	maxNode   int64
	maxStep   int64

	disabled bool
	strategy Strategy

	node  int64          // resolved node for static/statefulset (set once in New)
	lease *redisLease    // non-nil only for the redis strategy
	owned rueidis.Client // dedicated client to close on Stop (nil if shared/none)

	ready atomic.Bool

	// hot path — mu guards lastTime/step only
	mu       sync.Mutex
	lastTime int64
	step     int64
}

// New builds a Generator. It validates the bit layout, then for the static and
// statefulset strategies resolves the worker id immediately (so the generator is
// usable without Start); the redis strategy resolves its slot in Start.
//
// When Config.Enabled is false it returns a disabled generator whose lifecycle
// methods are no-ops and whose Generate refuses to run.
func New(cfg Config, deps Deps, opts ...Option) (*Generator, error) {
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

	g := &Generator{
		cfg:     cfg,
		deps:    deps,
		tracer:  tp.Tracer("github.com/tuanlao/pulse/pkg/snowflake"),
		nowFunc: time.Now,
	}

	if !cfg.Enabled {
		g.disabled = true
		return g, nil
	}

	if int(cfg.NodeBits)+int(cfg.StepBits) > 22 {
		return nil, fmt.Errorf("snowflake: node_bits + step_bits must be <= 22 to keep a >=41-bit timestamp, got %d+%d",
			cfg.NodeBits, cfg.StepBits)
	}
	g.epoch = cfg.Epoch
	g.nodeShift = cfg.StepBits
	g.timeShift = cfg.NodeBits + cfg.StepBits
	g.maxNode = int64(1)<<cfg.NodeBits - 1
	g.maxStep = int64(1)<<cfg.StepBits - 1
	g.strategy = cfg.WorkerID.Strategy

	switch cfg.WorkerID.Strategy {
	case StrategyStatic:
		node, _, err := staticResolver{id: cfg.WorkerID.Static}.resolve(context.Background(), g.maxNode)
		if err != nil {
			return nil, err
		}
		g.node = node
		g.deps.Metrics.setWorkerID(node)
		g.ready.Store(true)
	case StrategyStatefulSet:
		node, _, err := statefulSetResolver{podNameEnv: cfg.WorkerID.StatefulSet.PodNameEnv}.resolve(context.Background(), g.maxNode)
		if err != nil {
			return nil, err
		}
		g.node = node
		g.deps.Metrics.setWorkerID(node)
		g.ready.Store(true)
	case StrategyRedis:
		if err := g.setupRedis(); err != nil {
			return nil, err
		}
		// node resolved in Start
	default:
		return nil, fmt.Errorf("snowflake: unknown worker-id strategy %q", cfg.WorkerID.Strategy)
	}

	return g, nil
}

// setupRedis builds the slot locker (reusing a shared client or owning a
// dedicated one) for the redis strategy, mirroring pkg/cron's lock wiring.
func (g *Generator) setupRedis() error {
	rw := g.cfg.WorkerID.Redis
	client := g.deps.RedisClient
	if rc, ok := client.(*pulseredis.Client); ok && !rc.Enabled() {
		// A disabled *pulseredis.Client wraps a nil rueidis.Client; treat as absent.
		client = nil
	}
	if client == nil {
		c, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  []string{rw.Redis.Address},
			Username:     rw.Redis.Username,
			Password:     rw.Redis.Password,
			SelectDB:     rw.Redis.DB,
			DisableCache: true, // a lock client needs no client-side caching
		})
		if err != nil {
			return fmt.Errorf("snowflake: build redis worker-id client: %w", err)
		}
		client = c
		g.owned = c
	}

	poolSize := int(g.maxNode) + 1
	if rw.MaxID > 0 && rw.MaxID < poolSize {
		poolSize = rw.MaxID
	}

	g.lease = &redisLease{
		locker: pulseredis.NewLocker(client,
			pulseredis.WithLockKeyPrefix(rw.KeyPrefix),
			pulseredis.WithLockTTL(rw.TTL),
			pulseredis.WithLockTries(1),
		),
		poolSize:      poolSize,
		ttl:           rw.TTL,
		renewInterval: rw.RenewInterval,
		log:           g.deps.Logger,
		metrics:       g.deps.Metrics,
	}
	return nil
}

// Name implements lifecycle.Component.
func (g *Generator) Name() string { return "snowflake" }

// Start resolves the redis worker-id slot (and starts lease renewal) for the
// redis strategy; it is a no-op for the static/statefulset strategies, which
// resolved in New. It honors ctx for the bounded slot scan.
func (g *Generator) Start(ctx context.Context) error {
	if g.disabled || g.strategy != StrategyRedis {
		return nil
	}
	ctx, span := g.tracer.Start(ctx, "snowflake.acquire_worker_id")
	defer span.End()

	if err := g.lease.acquire(ctx); err != nil {
		return err
	}
	node := g.lease.node()
	g.deps.Metrics.setWorkerID(node)
	g.deps.Logger.Info("snowflake: acquired redis worker-id slot",
		zap.Int64("node", node), zap.Int("pool_size", g.lease.poolSize))
	g.ready.Store(true)
	return nil
}

// Stop releases the redis worker-id slot (and stops renewal) and closes any
// dedicated client this generator owns.
func (g *Generator) Stop(ctx context.Context) error {
	if g.disabled {
		return nil
	}
	var errs []error
	if g.lease != nil {
		if err := g.lease.stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if g.owned != nil {
		g.owned.Close()
	}
	return errors.Join(errs...)
}

// CheckReady implements lifecycle.ReadinessChecker: not ready until the worker id
// is resolved and not fenced, so /readyz gates traffic until ids can be minted
// safely.
func (g *Generator) CheckReady(context.Context) error {
	if g.disabled {
		return nil
	}
	if !g.ready.Load() {
		return ErrNotReady
	}
	if g.fenced() {
		return ErrLeaseLost
	}
	return nil
}

// Generate returns the next id. It panics if the generator cannot mint an id
// (disabled, not started, fenced, or a clock that went backwards beyond
// MaxClockDriftWait) — for the redis strategy prefer TryGenerate, which returns
// those as errors.
func (g *Generator) Generate() ID {
	id, err := g.TryGenerate()
	if err != nil {
		panic("snowflake: Generate: " + err.Error())
	}
	return id
}

// TryGenerate returns the next id or an error if one cannot be minted right now.
func (g *Generator) TryGenerate() (ID, error) {
	if g.disabled {
		return 0, ErrDisabled
	}
	if !g.ready.Load() {
		return 0, ErrNotReady
	}
	if g.fenced() {
		return 0, ErrLeaseLost
	}
	return g.generate()
}

func (g *Generator) generate() (ID, error) {
	node := g.currentNode()

	g.mu.Lock()
	defer g.mu.Unlock()

	now := g.nowFunc().UnixMilli()

	if now < g.lastTime {
		// Wall clock moved backwards (NTP step, VM migration). Briefly wait for it
		// to catch back up; if the drift exceeds MaxClockDriftWait, refuse rather
		// than mint an id with a smaller timestamp (which could collide).
		g.deps.Metrics.incClockBackwards()
		now = g.waitUntil(g.lastTime)
		if now < g.lastTime {
			return 0, fmt.Errorf("%w: clock is %dms behind", ErrClockBackwards, g.lastTime-now)
		}
	}

	if now == g.lastTime {
		g.step = (g.step + 1) & g.maxStep
		if g.step == 0 {
			// Sequence exhausted this millisecond; spin to the next one.
			g.deps.Metrics.incSeqExhausted()
			now = g.waitNextMillis(g.lastTime)
		}
	} else {
		g.step = 0
	}
	g.lastTime = now

	id := ID((now-g.epoch)<<g.timeShift | node<<g.nodeShift | g.step)
	g.deps.Metrics.incGenerated()
	return id, nil
}

// waitNextMillis busy-spins (sub-millisecond) until the clock advances past last.
func (g *Generator) waitNextMillis(last int64) int64 {
	now := g.nowFunc().UnixMilli()
	for now <= last {
		now = g.nowFunc().UnixMilli()
	}
	return now
}

// waitUntil waits up to MaxClockDriftWait (real time) for the wall clock to reach
// target, returning the latest reading (which may still be < target on timeout).
func (g *Generator) waitUntil(target int64) int64 {
	deadline := time.Now().Add(g.cfg.MaxClockDriftWait)
	for {
		now := g.nowFunc().UnixMilli()
		if now >= target {
			return now
		}
		if !time.Now().Before(deadline) {
			return now
		}
		time.Sleep(50 * time.Microsecond)
	}
}

// currentNode returns the worker id in effect: the immutable static/statefulset
// node, or the redis lease's current (re-acquirable) slot.
func (g *Generator) currentNode() int64 {
	if g.lease != nil {
		return g.lease.node()
	}
	return g.node
}

func (g *Generator) fenced() bool { return g.lease != nil && g.lease.isFenced() }

// WorkerID returns this generator's resolved worker (node) id.
func (g *Generator) WorkerID() int64 { return g.currentNode() }

// Ready reports whether the worker id is resolved (and the generator is not
// fenced) — i.e. whether Generate will succeed.
func (g *Generator) Ready() bool { return g.ready.Load() && !g.fenced() }

// MaxNode is the largest worker id this layout allows (2^NodeBits - 1).
func (g *Generator) MaxNode() int64 { return g.maxNode }

// MaxStep is the largest per-millisecond sequence this layout allows.
func (g *Generator) MaxStep() int64 { return g.maxStep }

// Time returns the timestamp embedded in id as Unix milliseconds.
func (g *Generator) Time(id ID) int64 { return int64(id)>>g.timeShift + g.epoch }

// TimeAt returns the timestamp embedded in id as a time.Time.
func (g *Generator) TimeAt(id ID) time.Time { return time.UnixMilli(g.Time(id)) }

// Node returns the worker id embedded in id.
func (g *Generator) Node(id ID) int64 { return int64(id) >> g.nodeShift & g.maxNode }

// Step returns the per-millisecond sequence embedded in id.
func (g *Generator) Step(id ID) int64 { return int64(id) & g.maxStep }
