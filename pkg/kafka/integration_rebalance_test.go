//go:build integration

package kafka

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

func TestIntegration_Rebalance_TwoConsumersSplitPartitions(t *testing.T) {
	topic := "pulse-it-rebal-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	const total = 40
	p := mustProducer(t, topic) // itConfig provisions with default partitions (4)

	var mu sync.Mutex
	byPartition := map[int32]map[string]bool{} // partition -> set of consumer labels
	handled := 0
	record := func(label string, part int32) {
		mu.Lock()
		if byPartition[part] == nil {
			byPartition[part] = map[string]bool{}
		}
		byPartition[part][label] = true
		handled++
		mu.Unlock()
	}

	mk := func(label string) *Consumer {
		c, err := NewConsumer(itConfig(topic), Deps{Logger: log.Nop()},
			WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
		if err != nil {
			t.Fatalf("NewConsumer %s: %v", label, err)
		}
		On(c, topic, func(_ context.Context, _ laneMsg, m *Message) error {
			record(label, m.Partition)
			return nil
		})
		if err := c.Start(context.Background()); err != nil {
			t.Fatalf("%s.Start: %v", label, err)
		}
		t.Cleanup(func() { _ = c.Stop(context.Background()) })
		return c
	}

	mk("A")
	mk("B")
	// Let the cooperative-sticky assignment stabilize before producing, so no
	// rebalance happens mid-consumption (which could duplicate at-least-once).
	time.Sleep(3 * time.Second)

	for i := 0; i < total; i++ {
		if err := Send(context.Background(), p, topic, strconv.Itoa(i), laneMsg{Lane: "x", Seq: i}); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	waitFor(t, 40*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handled == total
	}, "all records handled across both consumers")

	mu.Lock()
	defer mu.Unlock()
	partsByLabel := map[string]int{}
	for part, labels := range byPartition {
		if len(labels) != 1 {
			t.Fatalf("partition %d was handled by multiple consumers %v (partitions must be exclusively owned)", part, labels)
		}
		for l := range labels {
			partsByLabel[l]++
		}
	}
	if partsByLabel["A"] == 0 || partsByLabel["B"] == 0 {
		t.Fatalf("expected partitions split across both consumers, got %v", partsByLabel)
	}
}
