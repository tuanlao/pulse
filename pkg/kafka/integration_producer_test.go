//go:build integration

package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

// assertRoundtrip builds a producer+consumer from cfg, sends one event and
// asserts the consumer receives it. cfg.Producer.Topics must contain topic.
func assertRoundtrip(t *testing.T, cfg Config, topic string) {
	t.Helper()
	group := "g-" + uuid.NewString()
	got := make(chan itOrder, 1)

	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, e itOrder, _ *Message) error {
		got <- e
		return nil
	})
	startConsumer(t, c)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "rt", Amount: 1}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case e := <-got:
		if e.ID != "rt" {
			t.Fatalf("payload mismatch: %+v", e)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("roundtrip message not received")
	}
}

func TestIntegration_Producer_Compression(t *testing.T) {
	for _, comp := range []string{"gzip", "lz4", "zstd", "none"} {
		t.Run(comp, func(t *testing.T) {
			topic := "pulse-it-comp-" + comp + "-" + uuid.NewString()
			cfg := itConfig(topic)
			cfg.Producer.Compression = comp
			assertRoundtrip(t, cfg, topic)
		})
	}
}

func TestIntegration_Producer_RequiredAcks(t *testing.T) {
	for _, acks := range []string{"all", "leader", "none"} {
		t.Run(acks, func(t *testing.T) {
			topic := "pulse-it-acks-" + acks + "-" + uuid.NewString()
			cfg := itConfig(topic)
			cfg.Producer.RequiredAcks = acks
			assertRoundtrip(t, cfg, topic)
		})
	}
}

func TestIntegration_Producer_MultipleTopics(t *testing.T) {
	t1 := "pulse-it-mt1-" + uuid.NewString()
	t2 := "pulse-it-mt2-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	cfg := DefaultConfig()
	cfg.ServiceName = "kafka-it"
	cfg.Producer.Topics = []string{t1, t2}

	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	got := newIDSet()
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(t1, t2))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	h := func(_ context.Context, e itOrder, _ *Message) error { got.add(e.ID); return nil }
	On(c, t1, h)
	On(c, t2, h)
	startConsumer(t, c)

	if err := Send(context.Background(), p, t1, "k", itOrder{ID: "on-t1"}); err != nil {
		t.Fatalf("Send t1: %v", err)
	}
	if err := Send(context.Background(), p, t2, "k", itOrder{ID: "on-t2"}); err != nil {
		t.Fatalf("Send t2: %v", err)
	}
	waitFor(t, 30*time.Second, func() bool { return got.has("on-t1") && got.has("on-t2") }, "both topics consumed")
}

// TestIntegration_Producer_AsyncHooks: async Produce fires OnProduce on success
// and OnProduceError when the broker rejects an oversized record.
func TestIntegration_Producer_AsyncHooks(t *testing.T) {
	topic := "pulse-it-async-" + uuid.NewString()
	cfg := itConfig(topic)
	rec := newHookRecorder()

	p, err := NewProducer(cfg, Deps{Logger: log.Nop(), Hooks: rec.hooks()}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	// Normal async produce -> OnProduce.
	ok := NewMessage([]byte("k"), []byte(`{"id":"ok"}`))
	p.Produce(context.Background(), topic, ok)
	waitFor(t, 30*time.Second, func() bool { return rec.count("produce") >= 1 }, "OnProduce fires on success")

	// Oversized record (> broker max.message.bytes ~1 MiB) -> OnProduceError.
	big := NewMessage([]byte("k"), []byte(strings.Repeat("x", 3<<20)))
	p.Produce(context.Background(), topic, big)
	waitFor(t, 30*time.Second, func() bool { return rec.count("produceErr") >= 1 }, "OnProduceError fires on broker rejection")
}

// TestIntegration_Disabled_NoOp: a disabled component returns safe no-ops.
func TestIntegration_Disabled_NoOp(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	p, err := NewProducer(cfg, Deps{Logger: log.Nop()})
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("disabled producer Start: %v", err)
	}
	if err := Send(context.Background(), p, "whatever", "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("disabled producer Send should be a no-op, got: %v", err)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatalf("disabled producer Stop: %v", err)
	}

	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, "whatever", func(context.Context, itOrder, *Message) error {
		t.Error("disabled consumer must never call a handler")
		return nil
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("disabled consumer Start: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("disabled consumer Stop: %v", err)
	}
}
