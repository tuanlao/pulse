//go:build integration

package kafka

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

type laneMsg struct {
	Lane string `json:"lane"`
	Seq  int    `json:"seq"`
}

// produceLanes produces lanes×perLane records, interleaved by seq, keyed by lane
// (so each lane lands on a single partition and is ordered at the broker).
func produceLanes(t *testing.T, p *Producer, topic string, lanes []string, perLane int) {
	t.Helper()
	for seq := 0; seq < perLane; seq++ {
		for _, lane := range lanes {
			if err := Send(context.Background(), p, topic, lane, laneMsg{Lane: lane, Seq: seq}); err != nil {
				t.Fatalf("Send: %v", err)
			}
		}
	}
}

func TestIntegration_OrderedMode_PerPartitionOrderConcurrentAcross(t *testing.T) {
	topic := "pulse-it-ord-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	lanes := []string{"a", "b", "c", "d", "e", "f"}
	const perLane = 5
	total := len(lanes) * perLane

	ol := newOrderLog()
	ct := &concTracker{}

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithPartitions(4))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("ordered"), WithPartitions(4))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, e laneMsg, _ *Message) error {
		ct.enter()
		time.Sleep(30 * time.Millisecond)
		ol.record(e.Lane, e.Seq)
		ct.leave()
		return nil
	})
	startKafka(t, p, c)

	produceLanes(t, p, topic, lanes, perLane)
	waitFor(t, 40*time.Second, func() bool { return ol.totalRecorded() == total }, "all ordered records handled")

	ol.assertPerKeyAscending(t) // per-partition ordering preserved
	if peak := ct.peak(); peak < 2 {
		t.Fatalf("ordered mode should process different partitions concurrently, peak=%d", peak)
	}
}

func TestIntegration_KeyOrderedMode_SerialPerKeyConcurrentAcross(t *testing.T) {
	topic := "pulse-it-key-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	lanes := []string{"k1", "k2", "k3", "k4", "k5", "k6"}
	const perLane = 5
	total := len(lanes) * perLane

	ol := newOrderLog()
	ct := &concTracker{}

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithPartitions(4))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("key_ordered"), WithPartitions(4))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, e laneMsg, _ *Message) error {
		ct.enter()
		time.Sleep(30 * time.Millisecond)
		ol.record(e.Lane, e.Seq)
		ct.leave()
		return nil
	})
	startKafka(t, p, c)

	produceLanes(t, p, topic, lanes, perLane)
	waitFor(t, 40*time.Second, func() bool { return ol.totalRecorded() == total }, "all key_ordered records handled")

	ol.assertPerKeyAscending(t) // same key handled serially, in order
	if peak := ct.peak(); peak < 2 {
		t.Fatalf("key_ordered mode should process different keys concurrently, peak=%d", peak)
	}
}

func TestIntegration_UnorderedMode_FanOutConcurrency(t *testing.T) {
	topic := "pulse-it-unord-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	const total = 16

	ct := &concTracker{}
	var handled atomic.Int32

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithPartitions(4))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("unordered"))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ laneMsg, _ *Message) error {
		ct.enter()
		time.Sleep(50 * time.Millisecond)
		handled.Add(1)
		ct.leave()
		return nil
	})
	startKafka(t, p, c)

	for i := 0; i < total; i++ {
		if err := Send(context.Background(), p, topic, "", laneMsg{Lane: "x", Seq: i}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	waitFor(t, 40*time.Second, func() bool { return int(handled.Load()) == total }, "all unordered records handled")

	if peak := ct.peak(); peak < 2 {
		t.Fatalf("unordered mode should fan out concurrently, peak=%d", peak)
	}
}
