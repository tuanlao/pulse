package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/tuanlao/pulse/pkg/kafka/message"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeDeduper struct{ seen map[string]bool }

func (f *fakeDeduper) Seen(_ context.Context, id string) (bool, error) { return f.seen[id], nil }
func (f *fakeDeduper) Mark(_ context.Context, id string, _ time.Duration) error {
	f.seen[id] = true
	return nil
}

func msg(id string) *message.Message {
	m := &message.Message{Headers: message.NewHeaders(nil)}
	if id != "" {
		m.Headers.SetMessageID(id)
	}
	return m
}

func TestSafeHandlerRecoversPanic(t *testing.T) {
	c, _ := New(Config{}, Deps{})
	h := c.safeHandler(func(context.Context, *message.Message) error { panic("boom") })
	err := h(context.Background(), msg("x")) // must not panic
	if err == nil {
		t.Fatal("safeHandler should convert a panic into an error")
	}
}

func TestRequireHandlersFailsFast(t *testing.T) {
	c, _ := New(Config{Topics: []string{"orders"}}, Deps{})
	c.resolvedHandlers = map[string]message.Handler{}
	if err := c.requireHandlers(); err == nil {
		t.Fatal("requireHandlers should fail when an origin topic has no handler")
	}
	c.resolvedHandlers["orders"] = func(context.Context, *message.Message) error { return nil }
	if err := c.requireHandlers(); err != nil {
		t.Fatalf("requireHandlers unexpected error: %v", err)
	}
}

func TestSeenDuplicateEmptyID(t *testing.T) {
	c, _ := New(Config{}, Deps{})
	c.deduper = &fakeDeduper{seen: map[string]bool{"": true, "x": true}}
	if c.seenDuplicate(context.Background(), msg("")) {
		t.Fatal("a message with no id must never be treated as a duplicate")
	}
	if !c.seenDuplicate(context.Background(), msg("x")) {
		t.Fatal("id x should be seen")
	}
}

// TestPartitionTrackerContiguous is the headline correctness test: offsets are
// handled out of order (1,2,3 then 5,6, leaving a gap at 4) and the watermark
// must not advance past the gap; once 4 completes (e.g. forwarded to a retry
// topic) the watermark jumps to 6.
func TestPartitionTrackerContiguous(t *testing.T) {
	tr := newPartitionTracker()
	for i := int64(1); i <= 6; i++ {
		tr.Add(i)
	}
	mk := func(o int64) *kgo.Record { return &kgo.Record{Offset: o} }

	for _, o := range []int64{1, 2, 3} {
		hr, adv := tr.Done(mk(o))
		if !adv || hr.Offset != o {
			t.Fatalf("Done(%d): adv=%v hr=%v", o, adv, hr)
		}
	}
	if tr.Committed() != 3 {
		t.Fatalf("after 1,2,3 committed = %d, want 3", tr.Committed())
	}

	// 5 and 6 complete but 4 is still pending -> no advance past the gap.
	if _, adv := tr.Done(mk(5)); adv {
		t.Fatal("Done(5) advanced past the gap at 4")
	}
	if _, adv := tr.Done(mk(6)); adv {
		t.Fatal("Done(6) advanced past the gap at 4")
	}
	if tr.Committed() != 3 {
		t.Fatalf("with gap committed = %d, want 3", tr.Committed())
	}

	// 4 fills the gap -> watermark jumps straight to 6.
	hr, adv := tr.Done(mk(4))
	if !adv || hr.Offset != 6 {
		t.Fatalf("Done(4): adv=%v hr=%v, want advance to 6", adv, hr)
	}
	if tr.Committed() != 6 {
		t.Fatalf("after gap fill committed = %d, want 6", tr.Committed())
	}
}

func TestPartitionTrackerNonZeroBase(t *testing.T) {
	tr := newPartitionTracker()
	// Partition starts at offset 100.
	tr.Add(100)
	tr.Add(101)
	hr, adv := tr.Done(&kgo.Record{Offset: 100})
	if !adv || hr.Offset != 100 {
		t.Fatalf("Done(100): adv=%v hr=%v", adv, hr)
	}
	if tr.Committed() != 100 {
		t.Fatalf("committed = %d, want 100", tr.Committed())
	}
}

func TestPartitionTrackerDuplicateIgnored(t *testing.T) {
	tr := newPartitionTracker()
	tr.Add(1)
	tr.Done(&kgo.Record{Offset: 1})
	if _, adv := tr.Done(&kgo.Record{Offset: 1}); adv {
		t.Fatal("re-completing an already-committed offset advanced")
	}
}

func TestLaneForKeyDeterministic(t *testing.T) {
	a := laneForKey([]byte("user-42"))
	b := laneForKey([]byte("user-42"))
	if a != b {
		t.Errorf("laneForKey not deterministic: %d != %d", a, b)
	}
	if laneForKey(nil) != 0 || laneForKey([]byte{}) != 0 {
		t.Error("empty key should map to lane 0")
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	c.ApplyDefaults()
	if c.Mode != ModeUnordered {
		t.Errorf("Mode = %q, want unordered", c.Mode)
	}
	if c.CommitImmediately {
		t.Error("CommitImmediately should default false")
	}
	if c.Concurrency != 256 {
		t.Errorf("Concurrency = %d, want 256", c.Concurrency)
	}
}
