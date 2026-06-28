//go:build integration

// Integration tests for the kafka subsystem against a REAL broker. They cover the
// produce/consume roundtrip and the retry-topic + DLQ + dedup pipelines, asserting
// behavior through the message hooks rather than sleeping and guessing.
//
// They need a live Kafka broker. Run with the docker-compose stack:
//
//	make infra-up
//	go test -race -tags=integration ./pkg/kafka/... -v
//
// The broker address comes from KAFKA_BROKERS (default localhost:9092). Each test
// isolates itself with a uuid-suffixed topic + consumer group. They run the
// consumer in its DEFAULT at-least-once mode (watermark commits +
// BlockRebalanceOnPoll) and rely on the consumer auto-provisioning its retry
// tiers and DLQ.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

func itBrokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

// itConfig is the shared base config: the producer provisions the origin topic;
// the consumer auto-provisions its retry tiers and DLQ. A short drain keeps Stop
// snappy.
func itConfig(topic string) Config {
	cfg := DefaultConfig()
	cfg.ServiceName = "kafka-it"
	cfg.Producer.Topics = []string{topic}
	cfg.Consumer.DrainTimeout = 5 * time.Second
	return cfg
}

// startKafka starts the producer then the consumer, registering Stop on cleanup.
// A brand-new consumer group resets to the earliest offset (franz-go default), so
// records produced before the group finishes joining are still delivered.
func startKafka(t *testing.T, p *Producer, c *Consumer) {
	t.Helper()
	ctx := context.Background()
	if err := p.Start(ctx); err != nil {
		t.Fatalf("producer.Start (is kafka reachable at %v?): %v", itBrokers(), err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	if err := c.Start(ctx); err != nil {
		t.Fatalf("consumer.Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
}

type itOrder struct {
	ID     string `json:"id"`
	Amount int    `json:"amount"`
}

func TestIntegration_ProduceConsumeRoundtrip(t *testing.T) {
	topic := "pulse-it-rt-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	logger := log.Nop()

	got := make(chan itOrder, 1)
	hdr := make(chan *Message, 1)

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: logger}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(cfg, Deps{Logger: logger},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, e itOrder, m *Message) error {
		hdr <- m
		got <- e
		return nil
	})
	startKafka(t, p, c)

	if err := Send(context.Background(), p, topic, "order-1", itOrder{ID: "order-1", Amount: 42}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case e := <-got:
		if e.ID != "order-1" || e.Amount != 42 {
			t.Fatalf("payload mismatch: %+v", e)
		}
		m := <-hdr
		if string(m.Key) != "order-1" {
			t.Fatalf("key mismatch: %q", string(m.Key))
		}
		if m.MessageID() == "" {
			t.Fatal("missing message-id header")
		}
		if m.Source() != "kafka-it" {
			t.Fatalf("source header = %q, want kafka-it", m.Source())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the consumer to receive the message")
	}
}

func TestIntegration_RetryThenDLQ(t *testing.T) {
	topic := "pulse-it-retry-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	logger := log.Nop()

	var retries int32
	dlq := make(chan string, 1) // carries the DLQ error class

	hooks := Hooks{
		OnRetry: func(_ context.Context, _ *Message, _ int, _ time.Duration) {
			atomic.AddInt32(&retries, 1)
		},
		OnDLQ: func(_ context.Context, _ *Message, class, _ string) {
			select {
			case dlq <- class:
			default:
			}
		},
	}

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: logger}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	// Short backoffs keep the test fast; two tiers => two retries then DLQ.
	c, err := NewConsumer(cfg, Deps{Logger: logger, Hooks: hooks},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithBackoffs(200*time.Millisecond, 400*time.Millisecond))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return errors.New("always fails") // forces the retry -> DLQ path
	})
	startKafka(t, p, c)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case class := <-dlq:
		if class != string(ErrorRetriesExhausted) {
			t.Fatalf("DLQ class = %q, want %q", class, ErrorRetriesExhausted)
		}
		if n := atomic.LoadInt32(&retries); n < 1 {
			t.Fatalf("expected at least one retry before DLQ, got %d", n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the message to reach the DLQ")
	}
}

func TestIntegration_NonRetryableToDLQ(t *testing.T) {
	topic := "pulse-it-nonretry-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	logger := log.Nop()

	var retries int32
	dlq := make(chan string, 1)

	hooks := Hooks{
		OnRetry: func(_ context.Context, _ *Message, _ int, _ time.Duration) {
			atomic.AddInt32(&retries, 1)
		},
		OnDLQ: func(_ context.Context, _ *Message, class, _ string) {
			select {
			case dlq <- class:
			default:
			}
		},
	}

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: logger}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	c, err := NewConsumer(cfg, Deps{Logger: logger, Hooks: hooks},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		return NonRetryable(errors.New("poison")) // straight to DLQ, no retry
	})
	startKafka(t, p, c)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "x"}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case class := <-dlq:
		if class != string(ErrorNonRetryable) {
			t.Fatalf("DLQ class = %q, want %q", class, ErrorNonRetryable)
		}
		if n := atomic.LoadInt32(&retries); n != 0 {
			t.Fatalf("non-retryable must not retry, got %d retries", n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the non-retryable message to reach the DLQ")
	}
}

func TestIntegration_Dedup(t *testing.T) {
	topic := "pulse-it-dedup-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	logger := log.Nop()

	var handled int32
	skipped := make(chan struct{}, 1)

	hooks := Hooks{
		OnDedupeSkip: func(_ context.Context, _ *Message) {
			select {
			case skipped <- struct{}{}:
			default:
			}
		},
	}

	cfg := itConfig(topic)
	p, err := NewProducer(cfg, Deps{Logger: logger}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	// key_ordered + a shared key serializes the two records onto one lane, so the
	// first is fully processed (and its id remembered) before the duplicate runs —
	// making the local deduper's decision deterministic.
	c, err := NewConsumer(cfg, Deps{Logger: logger, Hooks: hooks},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic),
		WithMode("key_ordered"), WithDedup("local"))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	c.Register(topic, func(_ context.Context, _ *Message) error {
		atomic.AddInt32(&handled, 1)
		return nil
	})
	startKafka(t, p, c)

	// Two records, same message-id and same key => the second is a duplicate.
	payload, _ := json.Marshal(itOrder{ID: "dup"})
	for i := 0; i < 2; i++ {
		m := NewMessage([]byte("dup-key"), payload)
		m.Headers.SetMessageID("fixed-dedup-id")
		if err := p.ProduceSync(context.Background(), topic, m); err != nil {
			t.Fatalf("ProduceSync #%d: %v", i, err)
		}
	}

	select {
	case <-skipped:
		// Give the (already serialized) first handler a beat to settle, then assert
		// it ran exactly once.
		time.Sleep(500 * time.Millisecond)
		if n := atomic.LoadInt32(&handled); n != 1 {
			t.Fatalf("handler ran %d times, want exactly 1 (duplicate must be skipped)", n)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the duplicate to be skipped")
	}
}
