//go:build integration

// Shared helpers for the kafka integration tests (hook recording, topic
// observers, a redis client, and small concurrency/order trackers). All of these
// build on the broker provided by the docker-compose stack (KAFKA_BROKERS /
// REDIS_ADDR).
package kafka

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/redis"
)

func itRedisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

// hookRecorder captures message-lifecycle hook events thread-safely.
type hookRecorder struct {
	mu            sync.Mutex
	counts        map[string]int
	dlqClass      string
	dlqReason     string
	retryAttempts []int
	groupSkipIDs  []string
}

func newHookRecorder() *hookRecorder { return &hookRecorder{counts: map[string]int{}} }

func (r *hookRecorder) bump(name string) {
	r.mu.Lock()
	r.counts[name]++
	r.mu.Unlock()
}

func (r *hookRecorder) count(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name]
}

func (r *hookRecorder) dlq() (string, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dlqClass, r.dlqReason
}

func (r *hookRecorder) hooks() Hooks {
	return Hooks{
		OnConsume: func(context.Context, *Message) { r.bump("consume") },
		OnSuccess: func(context.Context, *Message, time.Duration) { r.bump("success") },
		OnError:   func(context.Context, *Message, error) { r.bump("error") },
		OnRetry: func(_ context.Context, _ *Message, attempt int, _ time.Duration) {
			r.mu.Lock()
			r.counts["retry"]++
			r.retryAttempts = append(r.retryAttempts, attempt)
			r.mu.Unlock()
		},
		OnDLQ: func(_ context.Context, _ *Message, class, reason string) {
			r.mu.Lock()
			r.counts["dlq"]++
			r.dlqClass = class
			r.dlqReason = reason
			r.mu.Unlock()
		},
		OnDedupeSkip: func(context.Context, *Message) { r.bump("dedupe") },
		OnGroupSkip: func(_ context.Context, m *Message) {
			r.mu.Lock()
			r.counts["groupskip"]++
			r.groupSkipIDs = append(r.groupSkipIDs, m.MessageID())
			r.mu.Unlock()
		},
		OnBackoffPause: func(context.Context, *Message, time.Time) { r.bump("backoff") },
		OnProduce:      func(context.Context, *Message) { r.bump("produce") },
		OnProduceError: func(context.Context, *Message, error) { r.bump("produceErr") },
	}
}

// startConsumer starts a consumer and registers Stop on cleanup. Use with
// mustProducer when a test has one producer and one (or more) consumers.
func startConsumer(t *testing.T, c *Consumer) {
	t.Helper()
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
}

// consumeInto starts a TERMINAL consumer on `topic` (no origin topics, so no
// retry/DLQ pipeline is attached) that forwards each received message to the
// returned channel. Used to observe retry-tier and DLQ topics.
func consumeInto(t *testing.T, topic, group string) <-chan *Message {
	t.Helper()
	ch := make(chan *Message, 128)
	cfg := DefaultConfig()
	cfg.ServiceName = "kafka-it-observer"
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group))
	if err != nil {
		t.Fatalf("observer NewConsumer: %v", err)
	}
	c.Register(topic, func(_ context.Context, m *Message) error {
		ch <- m
		return nil
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("observer Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return ch
}

// newRedisClient builds and starts a pulse redis client (a rueidis.Client) for
// the redis-backed dedup tests.
func newRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	c, err := redis.New(redis.DefaultConfig(), redis.Deps{}, redis.WithAddresses(itRedisAddr()))
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("redis.Start (is redis reachable at %s?): %v", itRedisAddr(), err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// concTracker records peak observed concurrency.
type concTracker struct {
	mu       sync.Mutex
	cur, max int
}

func (c *concTracker) enter() {
	c.mu.Lock()
	c.cur++
	if c.cur > c.max {
		c.max = c.cur
	}
	c.mu.Unlock()
}

func (c *concTracker) leave() {
	c.mu.Lock()
	c.cur--
	c.mu.Unlock()
}

func (c *concTracker) peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.max
}

// orderLog records the per-key sequence of handled records (handling order).
type orderLog struct {
	mu    sync.Mutex
	byKey map[string][]int
}

func newOrderLog() *orderLog { return &orderLog{byKey: map[string][]int{}} }

func (o *orderLog) record(key string, seq int) {
	o.mu.Lock()
	o.byKey[key] = append(o.byKey[key], seq)
	o.mu.Unlock()
}

func (o *orderLog) totalRecorded() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, v := range o.byKey {
		n += len(v)
	}
	return n
}

// assertPerKeyAscending fails if any key's recorded sequence is not strictly
// increasing (i.e. records for a key were reordered).
func (o *orderLog) assertPerKeyAscending(t *testing.T) {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	for key, seqs := range o.byKey {
		for i := 1; i < len(seqs); i++ {
			if seqs[i] <= seqs[i-1] {
				t.Fatalf("key %q handled out of order: %v", key, seqs)
			}
		}
	}
}
