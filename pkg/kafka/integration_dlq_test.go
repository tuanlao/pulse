//go:build integration

package kafka

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

// TestIntegration_DLQ_TerminalConsumptionAndHeaders: a non-retryable failure lands
// on the DLQ topic, which can be consumed terminally (a handler registered on the
// DLQ topic), carrying x-error-class and a truncated x-error-reason.
func TestIntegration_DLQ_TerminalConsumptionAndHeaders(t *testing.T) {
	topic := "pulse-it-dlq-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	cfg := itConfig(topic)
	longReason := strings.Repeat("x", 1000)

	p := mustProducer(t, topic)
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return NonRetryable(errors.New(longReason))
	})
	startConsumer(t, c)

	// Terminal consumption of the DLQ from a separate group.
	dlq := consumeInto(t, DLQTopic(cfg, topic), "dlq-obs-"+uuid.NewString())

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case m := <-dlq:
		if m.Headers.ErrorClass() != string(ErrorNonRetryable) {
			t.Fatalf("DLQ x-error-class = %q, want non_retryable", m.Headers.ErrorClass())
		}
		reason := m.Headers.ErrorReason()
		if reason == "" {
			t.Fatal("DLQ record missing x-error-reason")
		}
		if len(reason) > cfg.Retry.MaxErrorReasonBytes {
			t.Fatalf("x-error-reason len %d exceeds MaxErrorReasonBytes %d", len(reason), cfg.Retry.MaxErrorReasonBytes)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("no record observed on the DLQ topic")
	}
}

// TestIntegration_DLQ_Disabled_Drops: with the DLQ disabled, a terminal failure is
// dropped (offset advanced), not forwarded — OnDLQ never fires and the handler is
// not reprocessed in a loop.
func TestIntegration_DLQ_Disabled_Drops(t *testing.T) {
	topic := "pulse-it-dlqoff-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	cfg := itConfig(topic)
	rec := newHookRecorder()
	var calls atomic.Int32

	p := mustProducer(t, topic)
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), Hooks: rec.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithDLQ(false))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		calls.Add(1)
		return NonRetryable(errors.New("poison"))
	})
	startConsumer(t, c)

	dlq := consumeInto(t, DLQTopic(cfg, topic), "dlq-obs-"+uuid.NewString())

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	// Wait for the handler to run, then confirm nothing went to the DLQ and the
	// record was not reprocessed.
	waitFor(t, 30*time.Second, func() bool { return rec.count("error") >= 1 }, "handler failed once")
	select {
	case m := <-dlq:
		t.Fatalf("DLQ disabled but a record landed on the DLQ topic: %s", string(m.Value))
	case <-time.After(4 * time.Second):
	}
	if n := rec.count("dlq"); n != 0 {
		t.Fatalf("OnDLQ fired %d times with DLQ disabled, want 0", n)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("handler called %d times, want 1 (dropped, not reprocessed)", n)
	}
}
