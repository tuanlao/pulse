// Package consumer is the Kafka consumer-group component (lifecycle.Component).
// It dispatches records in one of three modes — unordered (high-concurrency
// fan-out over an ants pool), ordered (per-partition serial), and key_ordered
// (per-key serial, concurrent across keys) — with a contiguous-offset watermark
// for safe at-least-once commits, integrated dedup, the retry/DLQ pipeline, and
// graceful drain on shutdown.
package consumer

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/tuanlao/pulse/pkg/kafka/admin"
	"github.com/tuanlao/pulse/pkg/kafka/codec"
	"github.com/tuanlao/pulse/pkg/kafka/dedup"
	"github.com/tuanlao/pulse/pkg/kafka/internal/kclient"
	"github.com/tuanlao/pulse/pkg/kafka/message"
	kmetrics "github.com/tuanlao/pulse/pkg/kafka/metrics"
	"github.com/tuanlao/pulse/pkg/kafka/retry"
	ktrace "github.com/tuanlao/pulse/pkg/kafka/trace"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Processing modes.
const (
	ModeUnordered  = "unordered"
	ModeOrdered    = "ordered"
	ModeKeyOrdered = "key_ordered"
)

// Config is the consumer behavior configuration.
type Config struct {
	// GroupID is the consumer group. Required when consuming.
	GroupID string `mapstructure:"group_id"`
	// Topics are the origin topics (which get the retry/DLQ pipeline). Handlers
	// are bound by name with Register; an extra Register for a non-origin topic
	// (e.g. a DLQ) consumes it terminally.
	Topics []string `mapstructure:"topics"`
	// Mode is "unordered" (default), "ordered", or "key_ordered".
	Mode string `mapstructure:"mode"`
	// CommitImmediately, when true, commits offsets as records are polled
	// (at-most-once, high throughput, accepts message loss on crash). Default
	// false = watermark commits (at-least-once).
	CommitImmediately bool `mapstructure:"commit_immediately"`
	// Concurrency is the ants pool size (unordered) or the number of lanes
	// (ordered/key_ordered). Default 256.
	Concurrency int `mapstructure:"concurrency"`
	// MaxPollRecords bounds records returned per poll. Default 500.
	MaxPollRecords int `mapstructure:"max_poll_records"`
	// AutoCommitInterval is how often offsets are committed. Default 5s.
	AutoCommitInterval time.Duration `mapstructure:"auto_commit_interval"`
	// DrainTimeout bounds waiting for in-flight handlers on Stop. Default 30s.
	DrainTimeout time.Duration `mapstructure:"drain_timeout"`
	// SessionTimeout / RebalanceTimeout are passthroughs (0 = kgo default).
	SessionTimeout   time.Duration `mapstructure:"session_timeout"`
	RebalanceTimeout time.Duration `mapstructure:"rebalance_timeout"`
}

// DefaultConfig returns consumer defaults.
func DefaultConfig() Config {
	return Config{
		Mode:               ModeUnordered,
		CommitImmediately:  false,
		Concurrency:        256,
		MaxPollRecords:     500,
		AutoCommitInterval: 5 * time.Second,
		DrainTimeout:       30 * time.Second,
	}
}

// ApplyDefaults fills empty fields.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.Mode == "" {
		c.Mode = d.Mode
	}
	if c.Concurrency <= 0 {
		c.Concurrency = d.Concurrency
	}
	if c.MaxPollRecords <= 0 {
		c.MaxPollRecords = d.MaxPollRecords
	}
	if c.AutoCommitInterval <= 0 {
		c.AutoCommitInterval = d.AutoCommitInterval
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = d.DrainTimeout
	}
}

// Deps are the consumer's collaborators (built by the facade). The consumer
// builds its own client (the callbacks must close over it), retrier and
// scheduler internally.
type Deps struct {
	Conn           kclient.Config
	Admin          admin.Config
	Retry          retry.Config
	Tracing        ktrace.Config
	DedupTTL       time.Duration
	Codec          codec.Codec
	Deduper        dedup.Deduper
	Metrics        *kmetrics.Metrics
	Logger         *log.Logger
	TracerProvider trace.TracerProvider
	Hooks          message.Hooks
}

// Consumer consumes a group's topics and implements lifecycle.Component.
type Consumer struct {
	cfg  Config
	deps Deps

	codec    codec.Codec
	tracer   trace.Tracer
	deduper  dedup.Deduper
	metrics  *kmetrics.Metrics
	hooks    message.Hooks
	log      *log.Logger
	strategy string

	mu       sync.RWMutex
	handlers map[string]message.Handler

	// Built in Start.
	resolvedHandlers map[string]message.Handler // immutable snapshot read by the poll loop
	cl               *kgo.Client
	retrier          *retry.Retrier
	scheduler        *retry.DelayScheduler
	pool             *ants.Pool    // unordered
	lanes            []chan func() // ordered / key_ordered
	originSet        map[string]struct{}
	retryTierOrigin  map[string]string // tier topic -> origin
	blockRebalance   bool

	trackMu  sync.Mutex
	trackers map[topicPartition]*partitionTracker

	loopCtx    context.Context
	loopCancel context.CancelFunc
	loopDone   chan struct{}
	taskWG     sync.WaitGroup // unordered in-flight
	laneWG     sync.WaitGroup // lane workers

	disabled bool
}

// New builds a Consumer. The kgo client, retrier, scheduler and worker pool are
// built in Start (after handlers are registered), because the subscription
// depends on the registered topics and the group callbacks must close over the
// consumer.
func New(cfg Config, deps Deps) (*Consumer, error) {
	cfg.ApplyDefaults()
	deps.Retry.ApplyDefaults()
	if deps.Logger == nil {
		deps.Logger = log.Nop()
	}
	return &Consumer{
		cfg:      cfg,
		deps:     deps,
		codec:    codec.Or(deps.Codec),
		tracer:   ktrace.Tracer(deps.TracerProvider, deps.Tracing),
		deduper:  deps.Deduper,
		metrics:  deps.Metrics,
		hooks:    deps.Hooks,
		log:      deps.Logger,
		strategy: retry.EffectiveStrategy(deps.Retry, cfg.Mode),
		handlers: make(map[string]message.Handler),
		trackers: make(map[topicPartition]*partitionTracker),
	}, nil
}

// Disabled returns a no-op consumer (used when the kafka component is disabled).
func Disabled(logger *log.Logger) *Consumer {
	if logger == nil {
		logger = log.Nop()
	}
	return &Consumer{log: logger, disabled: true, codec: codec.JSON, handlers: map[string]message.Handler{}}
}

// Register binds a handler to a topic. Call before Start. Registering a topic
// outside Config.Topics (e.g. retry.DLQTopic(origin)) consumes it terminally.
func (c *Consumer) Register(topic string, h message.Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[topic] = h
}

// Codec returns the consumer's codec (used by the facade's typed On).
func (c *Consumer) Codec() codec.Codec { return c.codec }

// Name implements lifecycle.Component.
func (c *Consumer) Name() string { return "kafka-consumer:" + c.cfg.GroupID }

// Start builds the client (with the resolved subscription), provisions topics,
// and launches the poll loop. Non-blocking.
func (c *Consumer) Start(ctx context.Context) error {
	if c.disabled {
		return nil
	}
	if c.cfg.GroupID == "" {
		return fmt.Errorf("kafka consumer: group_id is required")
	}

	c.buildTopicSets()
	c.freezeHandlers() // snapshot once so the poll loop reads handlers lock-free
	if err := c.requireHandlers(); err != nil {
		return err
	}
	subscription := c.subscription()

	groupOpts := c.groupOpts(subscription)
	cl, err := kclient.Build(c.deps.Conn, groupOpts...)
	if err != nil {
		return err
	}
	c.cl = cl
	c.retrier = retry.NewRetrier(c.deps.Retry, c.cfg.GroupID, cl, c.metrics, c.hooks)
	c.scheduler = retry.NewDelayScheduler(cl, c.log, func(topic string, _ int32) {
		c.metrics.BackoffPaused(c.originFor(topic), c.cfg.GroupID, -1)
	})

	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka consumer: ping: %w", err)
	}
	if err := admin.Provision(ctx, cl, c.deps.Admin, c.provisionTopics(subscription), c.log); err != nil {
		return fmt.Errorf("kafka consumer: provision topics: %w", err)
	}

	c.startWorkers()

	c.loopCtx, c.loopCancel = context.WithCancel(context.Background())
	c.loopDone = make(chan struct{})
	lifecycle.SafeGo("kafka-consumer-poll", c.pollLoop, func(name string, r any, stack []byte) {
		c.log.Error("kafka consumer poll loop panicked", zap.String("name", name), zap.Any("panic", r))
	})
	return nil
}

// Stop drains in-flight work, commits, and closes the client.
func (c *Consumer) Stop(ctx context.Context) error {
	if c.disabled || c.cl == nil {
		return nil
	}
	// 1. Stop polling.
	if c.loopCancel != nil {
		c.loopCancel()
	}
	<-c.loopDone

	// 2. Cancel any paused retry timers.
	c.scheduler.Stop()

	// 3. Drain in-flight handlers (bounded by DrainTimeout).
	drainCtx, cancel := context.WithTimeout(ctx, c.cfg.DrainTimeout)
	defer cancel()
	c.stopWorkers(drainCtx)

	// 4. Commit final offsets.
	if !c.cfg.CommitImmediately {
		if err := c.cl.CommitMarkedOffsets(drainCtx); err != nil {
			c.log.Warn("kafka consumer: commit marked offsets on stop", zap.Error(err))
		}
	} else {
		_ = c.cl.CommitUncommittedOffsets(drainCtx)
	}

	// 5. Close.
	c.cl.Close()
	return nil
}

// pollLoop fetches and dispatches until the loop context is cancelled / the
// client is closed.
func (c *Consumer) pollLoop() {
	defer close(c.loopDone)
	for {
		if c.loopCtx.Err() != nil {
			return
		}
		fetches := c.cl.PollRecords(c.loopCtx, c.cfg.MaxPollRecords)
		closed := fetches.IsClientClosed()
		if !closed {
			fetches.EachError(func(t string, p int32, err error) {
				// A poll cancelled by shutdown surfaces as a context error, not a
				// real fetch failure — don't log it.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				c.log.Error("kafka fetch error", zap.String("topic", t), zap.Int32("partition", p), zap.Error(err))
			})
			fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
				c.dispatchPartition(ftp)
			})
		}
		// Release the poller gate after every poll — including the shutdown path.
		// With BlockRebalanceOnPoll a context-cancelled PollRecords still registers
		// a poller (it is "returning a fetch"); returning without AllowRebalance
		// leaves that poller dangling and deadlocks LeaveGroup inside Close().
		// AllowRebalance only touches in-memory state, so it is safe even here.
		if c.blockRebalance {
			c.cl.AllowRebalance()
		}
		if closed || c.loopCtx.Err() != nil {
			return
		}
	}
}

// groupOpts builds the consumer-group + producer (for retry/DLQ) options.
func (c *Consumer) groupOpts(subscription []string) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(c.cfg.GroupID),
		kgo.ConsumeTopics(subscription...),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
		kgo.AutoCommitInterval(c.cfg.AutoCommitInterval),
		// Produce-capable (the consumer forwards retries / DLQ records).
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	}
	if !c.cfg.CommitImmediately {
		opts = append(opts, kgo.AutoCommitMarks(), kgo.BlockRebalanceOnPoll())
		c.blockRebalance = true
	}
	if c.cfg.SessionTimeout > 0 {
		opts = append(opts, kgo.SessionTimeout(c.cfg.SessionTimeout))
	}
	if c.cfg.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(c.cfg.RebalanceTimeout))
	}
	return opts
}

// buildTopicSets computes the origin set and the retry-tier -> origin mapping.
func (c *Consumer) buildTopicSets() {
	c.originSet = make(map[string]struct{}, len(c.cfg.Topics))
	c.retryTierOrigin = make(map[string]string)
	namer := retry.NewNamer(c.deps.Retry)
	for _, o := range c.cfg.Topics {
		c.originSet[o] = struct{}{}
		if c.strategy == retry.StrategyNonBlocking {
			for _, tier := range namer.RetryTopics(o, c.cfg.GroupID) {
				c.retryTierOrigin[tier] = o
			}
		}
	}
}

// subscription is the registered topics plus (non-blocking) the retry tiers.
func (c *Consumer) subscription() []string {
	c.mu.RLock()
	set := make(map[string]struct{}, len(c.handlers))
	for t := range c.handlers {
		set[t] = struct{}{}
	}
	c.mu.RUnlock()
	for _, o := range c.cfg.Topics {
		set[o] = struct{}{}
	}
	for tier := range c.retryTierOrigin {
		set[tier] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	return out
}

// provisionTopics is the set of topics to create on Start: the subscription
// (origins + retry tiers) plus each origin's DLQ. The DLQ is terminal — produced
// to but never consumed — so it is not in the subscription; without provisioning
// it here a forward to the DLQ fails on a missing topic (the produce-capable
// client does not trigger broker auto-creation) and the record is reprocessed
// forever instead of landing in the DLQ.
func (c *Consumer) provisionTopics(subscription []string) []string {
	if !c.deps.Retry.DLQ.Enabled {
		return subscription
	}
	namer := retry.NewNamer(c.deps.Retry)
	set := make(map[string]struct{}, len(subscription))
	out := make([]string, 0, len(subscription)+len(c.cfg.Topics))
	for _, t := range subscription {
		set[t] = struct{}{}
		out = append(out, t)
	}
	for _, o := range c.cfg.Topics {
		dlq := namer.DLQTopic(o, c.cfg.GroupID)
		if _, ok := set[dlq]; !ok {
			set[dlq] = struct{}{}
			out = append(out, dlq)
		}
	}
	return out
}

// freezeHandlers snapshots the registered handlers once at Start so the poll loop
// can look them up without a lock (Register is only legal before Start). Each
// handler is wrapped so a panic becomes an error (recovered and routed through the
// retry/DLQ pipeline) instead of killing a worker goroutine or the poll loop.
func (c *Consumer) freezeHandlers() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.resolvedHandlers = make(map[string]message.Handler, len(c.handlers))
	for k, v := range c.handlers {
		c.resolvedHandlers[k] = c.safeHandler(v)
	}
}

// safeHandler wraps h so a panic is recovered into an error (logged with a stack)
// rather than unwinding the lane / poll-loop goroutine.
func (c *Consumer) safeHandler(h message.Handler) message.Handler {
	return func(ctx context.Context, m *message.Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("kafka: handler panicked: %v", r)
				c.log.ForContext(ctx).Error("kafka handler panicked",
					zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			}
		}()
		return h(ctx, m)
	}
}

// requireHandlers fails fast when a declared origin topic has no registered
// handler — otherwise a fetched record would invoke a nil handler and panic.
func (c *Consumer) requireHandlers() error {
	for _, t := range c.cfg.Topics {
		if c.resolvedHandlers[t] == nil {
			return fmt.Errorf("kafka consumer: no handler registered for topic %q (call Register before Start)", t)
		}
	}
	return nil
}

// originFor returns the origin a topic belongs to (itself if it is an origin or
// terminal, else the origin of a retry tier).
func (c *Consumer) originFor(topic string) string {
	if o, ok := c.retryTierOrigin[topic]; ok {
		return o
	}
	return topic
}

func (c *Consumer) onAssigned(context.Context, *kgo.Client, map[string][]int32) {}

// onRevoked cancels paused timers and commits the marked (handled) offsets before
// the partitions move. In-flight records not yet handled stay uncommitted and are
// reprocessed by the new owner (at-least-once; dedup mitigates).
func (c *Consumer) onRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	for topic, parts := range revoked {
		for _, p := range parts {
			c.scheduler.Revoke(topic, p)
			c.dropTracker(topic, p)
		}
	}
	if !c.cfg.CommitImmediately {
		if err := cl.CommitMarkedOffsets(ctx); err != nil {
			c.log.Warn("kafka consumer: commit on revoke", zap.Error(err))
		}
	}
}

// onLost drops state without committing (the connection is gone).
func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	for topic, parts := range lost {
		for _, p := range parts {
			c.scheduler.Revoke(topic, p)
			c.dropTracker(topic, p)
		}
	}
}

// tracker returns the per-partition tracker (watermark mode only), creating it
// lazily.
func (c *Consumer) tracker(tp topicPartition) *partitionTracker {
	if c.cfg.CommitImmediately {
		return nil
	}
	c.trackMu.Lock()
	defer c.trackMu.Unlock()
	t, ok := c.trackers[tp]
	if !ok {
		t = newPartitionTracker()
		c.trackers[tp] = t
	}
	return t
}

func (c *Consumer) dropTracker(topic string, partition int32) {
	c.trackMu.Lock()
	delete(c.trackers, topicPartition{topic, partition})
	c.trackMu.Unlock()
}

// advance marks a record handled and commits the contiguous watermark. It is a
// no-op in immediate mode (offsets are auto-committed on poll).
func (c *Consumer) advance(tr *partitionTracker, rec *kgo.Record) {
	if tr == nil {
		return
	}
	if hr, ok := tr.Done(rec); ok {
		c.cl.MarkCommitRecords(hr)
	}
}
