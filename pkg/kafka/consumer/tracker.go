package consumer

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartition keys a per-partition tracker.
type topicPartition struct {
	topic     string
	partition int32
}

// partitionTracker computes the highest CONTIGUOUS handled offset for a partition
// so that — even when records are handled out of order (the unordered /
// key_ordered modes) — only a gap-free prefix is committed. This is the core
// correctness mechanism: marking a record's offset directly would let a later
// offset commit ahead of an unfinished earlier one and silently drop it on crash.
//
// A record is "handled" (passed to Done) once it has been processed OR durably
// forwarded to a retry/DLQ topic — see the consumer's advance().
type partitionTracker struct {
	mu        sync.Mutex
	inited    bool
	committed int64 // highest contiguous handled offset
	done      map[int64]struct{}
	recs      map[int64]*kgo.Record
	highRec   *kgo.Record
}

func newPartitionTracker() *partitionTracker {
	return &partitionTracker{
		committed: -1,
		done:      make(map[int64]struct{}),
		recs:      make(map[int64]*kgo.Record),
	}
}

// Add registers an offset as received. It must be called in offset order (the
// poll loop dispatches a partition's records in order), which fixes the base from
// which the contiguous watermark advances.
func (t *partitionTracker) Add(offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.inited {
		t.committed = offset - 1
		t.inited = true
	}
}

// Done marks a record handled and advances the contiguous watermark. It returns
// the record at the new watermark and true when the watermark moved (the caller
// then MarkCommitRecords it).
func (t *partitionTracker) Done(rec *kgo.Record) (*kgo.Record, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	off := rec.Offset
	if t.inited && off <= t.committed {
		return nil, false // already committed (duplicate delivery)
	}
	t.done[off] = struct{}{}
	t.recs[off] = rec

	advanced := false
	for {
		next := t.committed + 1
		if _, ok := t.done[next]; !ok {
			break
		}
		t.highRec = t.recs[next]
		delete(t.done, next)
		delete(t.recs, next)
		t.committed = next
		advanced = true
	}
	if advanced {
		return t.highRec, true
	}
	return nil, false
}

// Committed returns the current contiguous watermark (for tests).
func (t *partitionTracker) Committed() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.committed
}
