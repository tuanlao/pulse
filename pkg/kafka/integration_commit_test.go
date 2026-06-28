//go:build integration

package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/log"
)

// idSet is a tiny thread-safe set of consumed ids.
type idSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newIDSet() *idSet { return &idSet{m: map[string]struct{}{}} }
func (s *idSet) add(id string) {
	s.mu.Lock()
	s.m[id] = struct{}{}
	s.mu.Unlock()
}
func (s *idSet) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.m)
}
func (s *idSet) has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[id]
	return ok
}

func mustProducer(t *testing.T, topic string) *Producer {
	t.Helper()
	p, err := NewProducer(itConfig(topic), Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start (kafka at %v?): %v", itBrokers(), err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	return p
}

// runCrashScenario seeds one message, lets consumer A receive it but block in the
// handler (simulating a crash before the offset is durably committed), stops A,
// then starts consumer B in the SAME group and reports whether B redelivers it.
func runCrashScenario(t *testing.T, commitImmediately bool) bool {
	t.Helper()
	topic := "pulse-it-commit-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	p := mustProducer(t, topic)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	acfg := itConfig(topic)
	acfg.Consumer.CommitImmediately = commitImmediately
	acfg.Consumer.DrainTimeout = 2 * time.Second
	// Short auto-commit so the at-most-once path's background commit fires while the
	// handler is still blocked (the graceful Stop commit can't, since the blocked
	// handler exhausts the drain budget). For at-least-once (AutoCommitMarks) nothing
	// is marked, so this commits nothing — the record stays uncommitted.
	acfg.Consumer.AutoCommitInterval = 500 * time.Millisecond
	a, err := NewConsumer(acfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer A: %v", err)
	}
	On(a, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release // block: the offset for this record is never marked/committed by A
		return nil
	})
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("A.Start: %v", err)
	}

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "crash"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("consumer A never received the seed message")
	}
	// Let the background auto-commit fire (at-most-once commits the polled offset
	// here even though the handler is still blocked).
	time.Sleep(1500 * time.Millisecond)
	// Stop A: drains for DrainTimeout (the blocked handler is abandoned).
	_ = a.Stop(context.Background())

	got := make(chan itOrder, 1)
	bcfg := itConfig(topic)
	bcfg.Consumer.CommitImmediately = commitImmediately
	b, err := NewConsumer(bcfg, Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer B: %v", err)
	}
	On(b, topic, func(_ context.Context, e itOrder, _ *Message) error {
		select {
		case got <- e:
		default:
		}
		return nil
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("B.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	select {
	case <-got:
		return true
	case <-time.After(10 * time.Second):
		return false
	}
}

func TestIntegration_AtLeastOnce_RedeliversUncommitted(t *testing.T) {
	if !runCrashScenario(t, false) {
		t.Fatal("at-least-once: a record uncommitted at crash must be redelivered to the new consumer")
	}
}

func TestIntegration_AtMostOnce_NoRedelivery(t *testing.T) {
	if runCrashScenario(t, true) {
		t.Fatal("at-most-once (commit_immediately): a record committed on poll must NOT be redelivered")
	}
}

func TestIntegration_ResumeFromCommittedOffset(t *testing.T) {
	topic := "pulse-it-resume-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	p := mustProducer(t, topic)

	// Consumer A consumes and commits the first batch, then stops.
	const n1 = 5
	gotA := newIDSet()
	a, err := NewConsumer(itConfig(topic), Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer A: %v", err)
	}
	On(a, topic, func(_ context.Context, e itOrder, _ *Message) error {
		gotA.add(e.ID)
		return nil
	})
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("A.Start: %v", err)
	}
	for i := 0; i < n1; i++ {
		if err := Send(context.Background(), p, topic, "k", itOrder{ID: "b1-" + uuid.NewString()}); err != nil {
			t.Fatalf("Send batch1: %v", err)
		}
	}
	waitFor(t, 30*time.Second, func() bool { return gotA.len() == n1 }, "A consumes batch1")
	_ = a.Stop(context.Background()) // commits the watermark for batch1

	// New messages produced after A committed.
	const n2 = 4
	want := map[string]bool{}
	for i := 0; i < n2; i++ {
		id := "b2-" + uuid.NewString()
		want[id] = true
		if err := Send(context.Background(), p, topic, "k", itOrder{ID: id}); err != nil {
			t.Fatalf("Send batch2: %v", err)
		}
	}

	// Consumer B in the SAME group resumes from the committed offset → only batch2.
	gotB := newIDSet()
	b, err := NewConsumer(itConfig(topic), Deps{Logger: log.Nop()}, WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer B: %v", err)
	}
	On(b, topic, func(_ context.Context, e itOrder, _ *Message) error {
		gotB.add(e.ID)
		return nil
	})
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("B.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	waitFor(t, 30*time.Second, func() bool { return gotB.len() == n2 }, "B consumes batch2")
	// Give a moment to catch any erroneous redelivery of batch1.
	time.Sleep(1 * time.Second)
	for id := range want {
		if !gotB.has(id) {
			t.Fatalf("B missing batch2 id %q", id)
		}
	}
	if gotB.len() != n2 {
		t.Fatalf("B consumed %d ids, want exactly %d (no batch1 redelivery)", gotB.len(), n2)
	}
}
