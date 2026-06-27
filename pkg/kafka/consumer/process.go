package consumer

import (
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/tuanlao/pulse/pkg/kafka/message"
	"github.com/tuanlao/pulse/pkg/kafka/retry"
	ktrace "github.com/tuanlao/pulse/pkg/kafka/trace"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// startWorkers builds the ants pool (unordered) or the lane goroutines
// (ordered / key_ordered).
func (c *Consumer) startWorkers() {
	switch c.cfg.Mode {
	case ModeOrdered, ModeKeyOrdered:
		c.lanes = make([]chan func(), c.cfg.Concurrency)
		for i := range c.lanes {
			ch := make(chan func(), 64)
			c.lanes[i] = ch
			c.laneWG.Add(1)
			go func(ch chan func()) {
				defer c.laneWG.Done()
				for task := range ch {
					task()
				}
			}(ch)
		}
	default: // unordered
		pool, _ := ants.NewPool(c.cfg.Concurrency, ants.WithPanicHandler(func(r any) {
			c.log.Error("kafka consumer worker panicked", zap.Any("panic", r))
		}))
		c.pool = pool
	}
}

// stopWorkers drains and releases the workers, bounded by ctx.
func (c *Consumer) stopWorkers(ctx context.Context) {
	switch c.cfg.Mode {
	case ModeOrdered, ModeKeyOrdered:
		for _, ch := range c.lanes {
			close(ch)
		}
		waitWG(ctx, &c.laneWG)
	default:
		waitWG(ctx, &c.taskWG)
		if c.pool != nil {
			c.pool.Release()
		}
	}
}

// waitWG waits for wg, returning early if ctx is done.
func waitWG(ctx context.Context, wg *sync.WaitGroup) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// dispatchPartition routes a partition's records by topic kind and mode.
func (c *Consumer) dispatchPartition(ftp kgo.FetchTopicPartition) {
	tp := topicPartition{ftp.Topic, ftp.Partition}
	tr := c.tracker(tp)

	switch {
	case c.isRetryTier(ftp.Topic):
		// Retry tiers are processed serially in the poll loop (low volume,
		// time-ordered, and the pause/seek must be coordinated with polling).
		c.processRetryPartition(ftp, tr)
	case c.isOrigin(ftp.Topic):
		c.dispatchOrigin(ftp, tr)
	default:
		// A registered non-origin topic (e.g. a DLQ being consumed) — terminal.
		c.processTerminalPartition(ftp, tr)
	}
}

func (c *Consumer) isOrigin(topic string) bool {
	_, ok := c.originSet[topic]
	return ok
}

func (c *Consumer) isRetryTier(topic string) bool {
	_, ok := c.retryTierOrigin[topic]
	return ok
}

// dispatchOrigin fans a partition's origin records to the configured worker model.
func (c *Consumer) dispatchOrigin(ftp kgo.FetchTopicPartition, tr *partitionTracker) {
	for _, rec := range ftp.Records {
		if tr != nil {
			tr.Add(rec.Offset)
		}
		r := rec
		task := func() { c.handleOrigin(r, tr) }
		switch c.cfg.Mode {
		case ModeOrdered:
			c.routeLane(int(r.Partition), task)
		case ModeKeyOrdered:
			c.routeLane(laneForKey(r.Key), task)
		default:
			c.submitPool(task)
		}
	}
}

// submitPool runs task on the ants pool (blocks when full = backpressure).
func (c *Consumer) submitPool(task func()) {
	c.taskWG.Add(1)
	if err := c.pool.Submit(func() { defer c.taskWG.Done(); task() }); err != nil {
		// Pool released (shutting down): run inline so the offset still advances.
		c.taskWG.Done()
		task()
	}
}

// routeLane sends task to a lane (blocks when the lane is full = backpressure).
// Same routing key -> same lane preserves order.
func (c *Consumer) routeLane(key int, task func()) {
	if key < 0 {
		key = -key
	}
	c.lanes[key%len(c.lanes)] <- task
}

func laneForKey(key []byte) int {
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32())
}

// consumeContext extracts the trace context, starts a consume span, and attaches
// the request-scoped logger — the common preamble for every record-processing
// path. The caller must End the returned span.
func (c *Consumer) consumeContext(rec *kgo.Record, topic string) (context.Context, trace.Span, *log.Logger) {
	ctx := ktrace.Extract(c.loopCtx, rec)
	ctx, span := ktrace.StartConsumeSpan(ctx, c.tracer, topic)
	l := c.log.ForContext(ctx)
	ctx = log.IntoContext(ctx, l)
	return ctx, span, l
}

// handleOrigin processes an origin record: dedup, handler, retry/DLQ, advance.
func (c *Consumer) handleOrigin(rec *kgo.Record, tr *partitionTracker) {
	origin := rec.Topic
	m := message.FromRecord(rec)
	ctx, span, l := c.consumeContext(rec, origin)
	defer span.End()

	if c.skipDuplicate(ctx, m, origin, tr, rec) {
		return
	}
	handler := c.resolvedHandlers[origin]
	if handler == nil {
		// Should be unreachable (requireHandlers fails fast at Start); guard anyway
		// so a missing handler never panics the worker.
		l.Error("kafka: no handler for origin topic", zap.String("topic", origin))
		c.advance(tr, rec)
		return
	}

	if c.strategy == retry.StrategyBlocking {
		c.handleBlocking(ctx, span, m, origin, handler, tr, rec)
	} else {
		c.handleNonBlocking(ctx, span, l, m, origin, handler, tr, rec)
	}
}

// handleNonBlocking runs the handler once; on failure it forwards to the next
// retry tier or the DLQ. Shared by the origin (unordered) path and the retry-tier
// path.
func (c *Consumer) handleNonBlocking(ctx context.Context, span trace.Span, l *log.Logger, m *message.Message, origin string, handler message.Handler, tr *partitionTracker, rec *kgo.Record) {
	c.hooks.FireConsume(ctx, m)
	c.metrics.InFlight(origin, c.cfg.GroupID, 1)
	start := time.Now()
	err := handler(ctx, m)
	d := time.Since(start)
	c.metrics.InFlight(origin, c.cfg.GroupID, -1)

	advance := true
	if err == nil {
		c.markSuccess(ctx, m, origin, d)
	} else {
		res, ferr := c.retrier.Forward(ctx, m, err)
		advance = res.Done
		c.recordFailure(ctx, l, m, origin, span, err, res, ferr, d)
	}
	if advance {
		c.advance(tr, rec)
	}
}

// handleBlocking runs the handler with in-place retries (order-preserving); the
// retrier forwards to the DLQ on exhaustion. Used by the ordered / key_ordered
// modes.
func (c *Consumer) handleBlocking(ctx context.Context, span trace.Span, m *message.Message, origin string, handler message.Handler, tr *partitionTracker, rec *kgo.Record) {
	c.hooks.FireConsume(ctx, m)
	c.metrics.InFlight(origin, c.cfg.GroupID, 1)
	start := time.Now()
	err, advance := c.retrier.RunInPlace(ctx, m, handler)
	d := time.Since(start)
	c.metrics.InFlight(origin, c.cfg.GroupID, -1)

	if err == nil {
		c.markSuccess(ctx, m, origin, d)
	} else {
		c.hooks.FireError(ctx, m, err)
		c.metrics.ObserveConsume(origin, c.cfg.GroupID, "dlq", d)
		span.RecordError(err)
	}
	if advance {
		c.advance(tr, rec)
	}
}

// markSuccess marks dedup and records the success.
func (c *Consumer) markSuccess(ctx context.Context, m *message.Message, origin string, d time.Duration) {
	if id := m.MessageID(); c.deduper != nil && id != "" {
		if err := c.deduper.Mark(ctx, id, c.deps.DedupTTL); err != nil {
			c.log.ForContext(ctx).Warn("kafka dedup mark failed", zap.Error(err))
		}
	}
	c.hooks.FireSuccess(ctx, m, d)
	c.metrics.ObserveConsume(origin, c.cfg.GroupID, "success", d)
}

// seenDuplicate reports whether m is a known duplicate. A message with no id is
// never deduplicable (treated as not-seen, so distinct id-less records are not
// collapsed onto the empty key); a Seen() backend error is logged and fails open.
func (c *Consumer) seenDuplicate(ctx context.Context, m *message.Message) bool {
	id := m.MessageID()
	if c.deduper == nil || id == "" {
		return false
	}
	seen, err := c.deduper.Seen(ctx, id)
	if err != nil {
		c.log.ForContext(ctx).Warn("kafka dedup check failed", zap.Error(err))
		return false
	}
	return seen
}

// recordFailure records a non-blocking handler failure that was forwarded.
func (c *Consumer) recordFailure(ctx context.Context, l *log.Logger, m *message.Message, origin string, span trace.Span, handlerErr error, res retry.ForwardResult, ferr error, d time.Duration) {
	c.hooks.FireError(ctx, m, handlerErr)
	status := "retry"
	if res.IsDLQ {
		status = "dlq"
	}
	c.metrics.ObserveConsume(origin, c.cfg.GroupID, status, d)
	span.RecordError(handlerErr)
	if ferr != nil {
		l.Error("kafka forward failed (will reprocess)", zap.String("origin", origin), zap.Error(ferr))
	}
}

// skipDuplicate skips and advances a record already handled (dedup). Returns true
// when skipped.
func (c *Consumer) skipDuplicate(ctx context.Context, m *message.Message, origin string, tr *partitionTracker, rec *kgo.Record) bool {
	if !c.seenDuplicate(ctx, m) {
		return false
	}
	c.hooks.FireDedupeSkip(ctx, m)
	c.metrics.IncDedupSkip(origin, c.cfg.GroupID)
	c.metrics.ObserveConsume(origin, c.cfg.GroupID, "dedup_skip", 0)
	c.advance(tr, rec)
	return true
}

// processRetryPartition processes a retry-tier partition serially: it skips
// wrong-group / duplicate records, pauses (via the scheduler) any record not yet
// due, and otherwise re-runs the origin handler.
func (c *Consumer) processRetryPartition(ftp kgo.FetchTopicPartition, tr *partitionTracker) {
	origin := c.retryTierOrigin[ftp.Topic]
	group := c.cfg.GroupID
	handler := c.resolvedHandlers[origin]
	now := time.Now()

	for _, rec := range ftp.Records {
		if tr != nil {
			tr.Add(rec.Offset)
		}
		m := message.FromRecord(rec)

		// Wrong-group skip (cheap, header-only) — an empty retry-group means any
		// group may process it.
		if rg := m.RetryGroup(); rg != "" && rg != group {
			c.hooks.FireGroupSkip(c.loopCtx, m)
			c.metrics.IncGroupSkip(origin, group)
			c.metrics.ObserveConsume(ftp.Topic, group, "group_skip", 0)
			c.advance(tr, rec)
			continue
		}

		if c.seenDuplicate(c.loopCtx, m) {
			c.hooks.FireDedupeSkip(c.loopCtx, m)
			c.metrics.IncDedupSkip(origin, group)
			c.metrics.ObserveConsume(ftp.Topic, group, "dedup_skip", 0)
			c.advance(tr, rec)
			continue
		}

		// Not yet due: pause the partition and seek back; the record is re-read
		// when the scheduler resumes it. Due times are monotonic within a tier, so
		// stop processing the rest of this partition this round.
		if due := m.RetryDueAt(); !due.IsZero() && now.Before(due) {
			c.hooks.FireBackoffPause(c.loopCtx, m, due)
			c.metrics.BackoffPaused(origin, group, 1)
			c.scheduler.Schedule(retry.RetryPosition{
				Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Epoch: rec.LeaderEpoch,
			}, due)
			return
		}

		c.handleRetryRecord(rec, m, origin, handler, tr)
	}
}

// handleRetryRecord runs the origin handler for a due retry record; on failure it
// forwards to the next tier or the DLQ (the same non-blocking core as an origin
// record in unordered mode).
func (c *Consumer) handleRetryRecord(rec *kgo.Record, m *message.Message, origin string, handler message.Handler, tr *partitionTracker) {
	ctx, span, l := c.consumeContext(rec, rec.Topic)
	defer span.End()
	c.handleNonBlocking(ctx, span, l, m, origin, handler, tr, rec)
}

// processTerminalPartition runs the registered handler for a non-origin topic
// (e.g. a DLQ being consumed). Failures are logged but not retried/forwarded.
func (c *Consumer) processTerminalPartition(ftp kgo.FetchTopicPartition, tr *partitionTracker) {
	group := c.cfg.GroupID
	handler := c.resolvedHandlers[ftp.Topic]
	if handler == nil {
		return
	}

	for _, rec := range ftp.Records {
		if tr != nil {
			tr.Add(rec.Offset)
		}
		m := message.FromRecord(rec)
		ctx, span, l := c.consumeContext(rec, rec.Topic)

		c.hooks.FireConsume(ctx, m)
		start := time.Now()
		err := handler(ctx, m)
		d := time.Since(start)
		if err == nil {
			c.hooks.FireSuccess(ctx, m, d)
			c.metrics.ObserveConsume(rec.Topic, group, "success", d)
		} else {
			c.hooks.FireError(ctx, m, err)
			c.metrics.ObserveConsume(rec.Topic, group, "error", d)
			span.RecordError(err)
			l.Error("kafka terminal handler failed", zap.String("topic", rec.Topic), zap.Error(err))
		}
		span.End()
		c.advance(tr, rec)
	}
}
