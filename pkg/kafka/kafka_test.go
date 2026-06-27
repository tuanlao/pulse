package kafka

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestDefaultConfigApplyDefaults(t *testing.T) {
	c := DefaultConfig()
	c.applyDefaults()
	c.applyDefaults() // idempotent

	if !c.Enabled {
		t.Error("Enabled should default true")
	}
	if c.Admin.Partitions != 4 {
		t.Errorf("Admin.Partitions = %d, want 4", c.Admin.Partitions)
	}
	if !c.Admin.AutoCreate {
		t.Error("Admin.AutoCreate should default true")
	}
	if c.Consumer.Mode != "unordered" {
		t.Errorf("Consumer.Mode = %q, want unordered", c.Consumer.Mode)
	}
	if c.Consumer.CommitImmediately {
		t.Error("CommitImmediately should default false")
	}
	if len(c.Retry.Backoffs) != 3 {
		t.Errorf("Retry.Backoffs len = %d, want 3", len(c.Retry.Backoffs))
	}
	if c.Dedup.Enabled {
		t.Error("Dedup should default off")
	}
}

func TestDLQTopic(t *testing.T) {
	c := DefaultConfig()
	if got := DLQTopic(c, "orders"); got != "orders.dlq" {
		t.Errorf("DLQTopic = %q, want orders.dlq", got)
	}
}

func TestNewMetrics(t *testing.T) {
	c := DefaultConfig()
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(c.Metrics, reg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if m == nil {
		t.Fatal("NewMetrics returned nil for enabled metrics")
	}
	if m.Registry() != reg {
		t.Error("Registry() mismatch")
	}

	c.Metrics.Enabled = false
	m2, err := NewMetrics(c.Metrics, reg)
	if err != nil {
		t.Fatalf("NewMetrics(disabled): %v", err)
	}
	if m2 != nil {
		t.Error("disabled metrics should return nil")
	}
}

func TestDisabledComponents(t *testing.T) {
	c := DefaultConfig()
	c.Enabled = false

	p, err := NewProducer(c, Deps{})
	if err != nil {
		t.Fatalf("NewProducer(disabled): %v", err)
	}
	if p.Name() != "kafka-producer" {
		t.Errorf("producer Name = %q", p.Name())
	}
	// A disabled producer's Send is a no-op (no client, no panic).
	if err := p.Send(context.Background(), "orders", "k", map[string]int{"x": 1}); err != nil {
		t.Errorf("disabled Send: %v", err)
	}

	cons, err := NewConsumer(c, Deps{})
	if err != nil {
		t.Fatalf("NewConsumer(disabled): %v", err)
	}
	// On must be safe on a disabled consumer (codec present, registers a handler).
	On(cons, "orders", func(_ context.Context, _ map[string]any, _ *Message) error { return nil })
}
