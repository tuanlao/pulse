//go:build integration

package kafka

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kadm"
)

// TestIntegration_Admin_AutoCreatePartitionsAndConfigs: the producer provisions
// its topics with the configured partition count and extra topic configs.
func TestIntegration_Admin_AutoCreatePartitionsAndConfigs(t *testing.T) {
	topic := "pulse-it-admin-" + uuid.NewString()
	cfg := itConfig(topic)
	cfg.Admin.Partitions = 3
	cfg.Admin.Configs = map[string]string{"retention.ms": "3600000"}

	p, err := NewProducer(cfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	adm := kadm.NewClient(p.Client())
	ctx := context.Background()

	td, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if n := len(td[topic].Partitions); n != 3 {
		t.Fatalf("topic %q has %d partitions, want 3", topic, n)
	}

	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatalf("DescribeTopicConfigs: %v", err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		t.Fatalf("config resource for %q: %v", topic, err)
	}
	var retention string
	for _, c := range rc.Configs {
		if c.Key == "retention.ms" && c.Value != nil {
			retention = *c.Value
		}
	}
	if retention != "3600000" {
		t.Fatalf("retention.ms = %q, want 3600000", retention)
	}
}

// TestIntegration_Admin_ValidateFailsFast: with auto_create disabled, starting a
// consumer whose subscription topics do not exist fails fast.
func TestIntegration_Admin_ValidateFailsFast(t *testing.T) {
	topic := "pulse-it-missing-" + uuid.NewString()
	cfg := DefaultConfig()
	cfg.ServiceName = "kafka-it"
	cfg.Admin.AutoCreate = false

	c, err := NewConsumer(cfg, Deps{Logger: log.Nop()},
		WithBrokers(itBrokers()...), WithGroupID("g-"+uuid.NewString()), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(context.Context, itOrder, *Message) error { return nil })
	if err := c.Start(context.Background()); err == nil {
		_ = c.Stop(context.Background())
		t.Fatal("expected Start to fail fast when auto_create is off and topics are missing")
	}
}
