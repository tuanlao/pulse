//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

// produceID produces one record with an explicit message-id (so duplicates can be
// constructed deterministically).
func produceID(t *testing.T, p *Producer, topic, key, msgID string, payload any) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	m := NewMessage([]byte(key), data)
	m.Headers.SetMessageID(msgID)
	if err := p.ProduceSync(context.Background(), topic, m); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
}

// TestIntegration_Dedup_Redis_SharedAcrossPods: the redis deduper is shared, so a
// duplicate message-id processed by one pod is skipped by another pod (here a
// second consumer started after the first marked the id).
func TestIntegration_Dedup_Redis_SharedAcrossPods(t *testing.T) {
	topic := "pulse-it-rdedup-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	prefix := "pulse:it:dedup:" + uuid.NewString()
	const dupID = "dup-X"
	rc := newRedisClient(t)
	p := mustProducer(t, topic)

	// Pod A processes the first occurrence and marks it in shared redis.
	var aHandled atomic.Int32
	acfg := itConfig(topic)
	acfg.Dedup.Redis.KeyPrefix = prefix
	a, err := NewConsumer(acfg, Deps{Logger: log.Nop(), RedisClient: rc},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic), WithDedup("redis"))
	if err != nil {
		t.Fatalf("NewConsumer A: %v", err)
	}
	On(a, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		aHandled.Add(1)
		return nil
	})
	startConsumer(t, a)

	produceID(t, p, topic, "k", dupID, itOrder{ID: "first"})
	waitFor(t, 30*time.Second, func() bool { return aHandled.Load() == 1 }, "pod A processes the first occurrence")
	_ = a.Stop(context.Background()) // commits its offset; the id stays marked in redis

	// A duplicate (same message-id) produced after A committed.
	produceID(t, p, topic, "k", dupID, itOrder{ID: "second"})

	// Pod B (same group, same shared redis prefix) resumes after A's offset, sees
	// only the duplicate, and skips it via the shared deduper.
	recB := newHookRecorder()
	var bHandled atomic.Int32
	bcfg := itConfig(topic)
	bcfg.Dedup.Redis.KeyPrefix = prefix
	b, err := NewConsumer(bcfg, Deps{Logger: log.Nop(), RedisClient: rc, Hooks: recB.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic), WithDedup("redis"))
	if err != nil {
		t.Fatalf("NewConsumer B: %v", err)
	}
	On(b, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		bHandled.Add(1)
		return nil
	})
	startConsumer(t, b)

	waitFor(t, 30*time.Second, func() bool { return recB.count("dedupe") >= 1 }, "pod B skips the shared duplicate")
	time.Sleep(500 * time.Millisecond)
	if n := bHandled.Load(); n != 0 {
		t.Fatalf("pod B handler ran %d times, want 0 (duplicate must be skipped via shared redis)", n)
	}
}

// TestIntegration_Dedup_TTLExpiry_Reprocesses: once a message-id's dedup entry
// expires (TTL), the same id is processed again instead of skipped.
func TestIntegration_Dedup_TTLExpiry_Reprocesses(t *testing.T) {
	topic := "pulse-it-dttl-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	const dupID = "ttl-X"
	rc := newRedisClient(t)
	p := mustProducer(t, topic)

	rec := newHookRecorder()
	var handled atomic.Int32
	cfg := itConfig(topic)
	cfg.Dedup.Redis.KeyPrefix = "pulse:it:dedup:" + uuid.NewString()
	cfg.Dedup.TTL = 1 * time.Second
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), RedisClient: rc, Hooks: rec.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic), WithDedup("redis"))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		handled.Add(1)
		return nil
	})
	startConsumer(t, c)

	produceID(t, p, topic, "k", dupID, itOrder{ID: "first"})
	waitFor(t, 30*time.Second, func() bool { return handled.Load() == 1 }, "first occurrence processed")

	// Let the dedup entry expire, then send the same id again.
	time.Sleep(2 * time.Second)
	produceID(t, p, topic, "k", dupID, itOrder{ID: "second"})
	waitFor(t, 30*time.Second, func() bool { return handled.Load() == 2 }, "duplicate reprocessed after TTL expiry")

	if n := rec.count("dedupe"); n != 0 {
		t.Fatalf("OnDedupeSkip fired %d times, want 0 (TTL expired so no skip)", n)
	}
}
