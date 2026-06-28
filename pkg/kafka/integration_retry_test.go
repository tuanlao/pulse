//go:build integration

package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

// TestIntegration_BlockingRetry_InPlaceThenDLQ: ordered mode resolves to the
// blocking strategy — the handler is retried IN PLACE (same record) up to
// BlockingMaxAttempts, then the record is DLQ'd.
func TestIntegration_BlockingRetry_InPlaceThenDLQ(t *testing.T) {
	topic := "pulse-it-bretry-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	rec := newHookRecorder()
	var calls atomic.Int32

	cfg := itConfig(topic)
	cfg.Retry.Backoffs = []time.Duration{100 * time.Millisecond, 100 * time.Millisecond}
	cfg.Retry.BlockingMaxAttempts = 2 // 1 initial + 2 in-place retries = 3 calls
	p := mustProducer(t, topic)
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), Hooks: rec.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("ordered")) // -> blocking strategy
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		calls.Add(1)
		return errors.New("boom")
	})
	startConsumer(t, c)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool { return rec.count("dlq") == 1 }, "message DLQ'd after in-place retries")

	if got := calls.Load(); got != 3 { // 1 initial + 2 in-place retries
		t.Fatalf("handler called %d times, want 3 (1 initial + BlockingMaxAttempts)", got)
	}
	if class, _ := rec.dlq(); class != string(ErrorRetriesExhausted) {
		t.Fatalf("DLQ class = %q, want retries_exhausted", class)
	}
}

// TestIntegration_NonBlockingRetry_RetryTopicHeaders: unordered mode resolves to
// non-blocking — a failed record is forwarded to a retry-tier topic stamped with
// x-retry-count and the origin coordinates, then eventually DLQ'd.
func TestIntegration_NonBlockingRetry_RetryTopicHeaders(t *testing.T) {
	topic := "pulse-it-nbretry-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	rec := newHookRecorder()

	cfg := itConfig(topic)
	p := mustProducer(t, topic)
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), Hooks: rec.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("unordered"), // -> non-blocking strategy (retry topics)
		WithBackoffs(1*time.Second, 2*time.Second))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return errors.New("boom")
	})
	startConsumer(t, c)

	// Observe the first retry tier from a SEPARATE group (terminal, no due-time
	// pause) so we can inspect the forwarded record's headers immediately.
	obs := consumeInto(t, topic+".retry.1s", "obs-"+uuid.NewString())

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case m := <-obs:
		if m.Headers.RetryCount() != 1 {
			t.Fatalf("retry-tier record x-retry-count = %d, want 1", m.Headers.RetryCount())
		}
		if m.Headers.OriginTopic() != topic {
			t.Fatalf("x-origin-topic = %q, want %q", m.Headers.OriginTopic(), topic)
		}
		if m.Headers.RetryDueAt().IsZero() {
			t.Fatal("retry-tier record missing x-retry-due-at")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no record observed on the first retry tier")
	}

	waitFor(t, 30*time.Second, func() bool { return rec.count("dlq") == 1 }, "record eventually DLQ'd")
}

// TestIntegration_RetryDelayFormat_Ms: delay_format "ms" names retry tiers with
// millisecond delays (e.g. {origin}.retry.500) instead of "human" (500ms).
func TestIntegration_RetryDelayFormat_Ms(t *testing.T) {
	topic := "pulse-it-msfmt-" + uuid.NewString()
	group := "g-" + uuid.NewString()

	cfg := itConfig(topic)
	cfg.Retry.DelayFormat = "ms"
	p := mustProducer(t, topic)
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("unordered"), WithBackoffs(500*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return errors.New("boom")
	})
	startConsumer(t, c)

	obs := consumeInto(t, topic+".retry.500", "obs-"+uuid.NewString()) // ms-formatted name

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-obs:
		// ok: a record landed on the ms-named retry tier
	case <-time.After(30 * time.Second):
		t.Fatalf("expected a record on the ms-formatted retry tier %q", topic+".retry.500")
	}
}

// TestIntegration_ScopeToGroup_OtherGroupSkips: with scope_to_group, a retry
// record stamped for group A is skipped (OnGroupSkip) by a consumer in group B.
func TestIntegration_ScopeToGroup_OtherGroupSkips(t *testing.T) {
	topic := "pulse-it-scope-" + uuid.NewString()
	groupA := "gA-" + uuid.NewString()
	groupB := "gB-" + uuid.NewString()
	p := mustProducer(t, topic)

	cfgA := itConfig(topic)
	cfgA.Retry.ScopeToGroup = true
	a, err := NewConsumer(cfgA, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(groupA), WithTopics(topic),
		WithMode("unordered"), WithBackoffs(500*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConsumer A: %v", err)
	}
	On(a, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return errors.New("boom") // A fails -> forwards a retry scoped to groupA
	})

	recB := newHookRecorder()
	cfgB := itConfig(topic)
	cfgB.Retry.ScopeToGroup = true
	b, err := NewConsumer(cfgB, Deps{Logger: log.Nop(), Hooks: recB.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(groupB), WithTopics(topic),
		WithMode("unordered"), WithBackoffs(500*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConsumer B: %v", err)
	}
	On(b, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return nil // B succeeds on origin; it should SKIP A's scoped retry record
	})

	startConsumer(t, a)
	startConsumer(t, b)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool { return recB.count("groupskip") >= 1 },
		"group B skips group A's scoped retry record")
}
