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

// newEntry is a small helper: a per-consumer config with the given group/topics
// (other fields default via applyDefaults).
func newEntry(group string, topics ...string) ConsumerConfig {
	cc := ConsumerConfig{}
	cc.GroupID = group
	cc.Topics = topics
	return cc
}

func TestConsumersApplyDefaults(t *testing.T) {
	c := DefaultConfig()
	audit := newEntry("g-audit", "orders")
	audit.Mode = "ordered"      // explicit per-consumer override is preserved
	audit.Retry.Enabled = false // diverge from the orders consumer
	c.Consumers = map[string]ConsumerConfig{
		"orders": newEntry("g-orders", "orders"),
		"audit":  audit,
	}
	c.applyDefaults()
	c.applyDefaults() // idempotent

	o := c.Consumers["orders"]
	if o.Mode != "unordered" {
		t.Errorf("orders Mode = %q, want unordered (default)", o.Mode)
	}
	if o.Concurrency != 256 {
		t.Errorf("orders Concurrency = %d, want 256 (default)", o.Concurrency)
	}
	if len(o.Retry.Backoffs) != 3 {
		t.Errorf("orders Retry.Backoffs = %d, want 3 (default)", len(o.Retry.Backoffs))
	}
	if o.Dedup.TTL <= 0 {
		t.Error("orders Dedup.TTL should be defaulted")
	}
	if a := c.Consumers["audit"]; a.Mode != "ordered" || a.Retry.Enabled {
		t.Errorf("audit overrides not preserved: mode=%q retry.enabled=%v", a.Mode, a.Retry.Enabled)
	}
}

func TestNewConsumers(t *testing.T) {
	c := DefaultConfig()
	c.Consumers = map[string]ConsumerConfig{
		"orders": newEntry("grp-orders", "orders"),
		"audit":  newEntry("grp-audit", "orders"),
	}
	set, err := NewConsumers(c, Deps{})
	if err != nil {
		t.Fatalf("NewConsumers: %v", err)
	}
	if set.Len() != 2 {
		t.Fatalf("Len = %d, want 2", set.Len())
	}
	got, ok := set.Get("orders")
	if !ok {
		t.Fatal("Get(orders): not found")
	}
	// Name() embeds the per-consumer group id — proof each got its own config.
	if got.Name() != "kafka-consumer:grp-orders" {
		t.Errorf("orders Name = %q, want kafka-consumer:grp-orders", got.Name())
	}
	if _, ok := set.Get("missing"); ok {
		t.Error("Get(missing) should be false")
	}
	if names := set.Names(); len(names) != 2 || names[0] != "audit" || names[1] != "orders" {
		t.Errorf("Names = %v, want [audit orders] (sorted)", names)
	}
	if len(set.Components()) != 2 {
		t.Errorf("Components len = %d, want 2", len(set.Components()))
	}

	// MustGet panics on an undeclared name.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("MustGet(nope) should panic")
			}
		}()
		_ = set.MustGet("nope")
	}()
}

func TestNewConsumersDisabled(t *testing.T) {
	c := DefaultConfig()
	c.Enabled = false
	c.Consumers = map[string]ConsumerConfig{"x": newEntry("g", "t")}
	set, err := NewConsumers(c, Deps{})
	if err != nil {
		t.Fatalf("NewConsumers(disabled): %v", err)
	}
	x, ok := set.Get("x")
	if !ok || x == nil {
		t.Fatal("Get(x) should return a (disabled) consumer")
	}
	// A disabled consumer's Start is a no-op (no broker needed).
	if err := x.Start(context.Background()); err != nil {
		t.Errorf("disabled Start: %v", err)
	}
}

func TestConsumerConfigDLQTopic(t *testing.T) {
	cc := DefaultConsumerConfig()
	cc.GroupID = "g"
	if got := cc.DLQTopic("orders"); got != "orders.dlq" {
		t.Errorf("ConsumerConfig.DLQTopic = %q, want orders.dlq", got)
	}
}
