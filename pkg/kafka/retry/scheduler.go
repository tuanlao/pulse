package retry

import (
	"sync"
	"time"

	"github.com/tuanlao/pulse/pkg/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// RetryPosition is the minimal partition position the DelayScheduler needs to
// pause and re-read a not-yet-due retry record. It is deliberately Kafka-message
// agnostic (no *kgo.Record / Message), so the scheduler is reusable and easy to
// test.
type RetryPosition struct {
	Topic     string
	Partition int32
	Offset    int64
	Epoch     int32
}

type tp struct {
	topic     string
	partition int32
}

// DelayScheduler enforces a retry record's due time the Spring-Kafka way: it
// pauses the record's partition and seeks back to it, then resumes the partition
// when the due time elapses (the record is re-fetched and, now due, processed).
// Because each retry tier has a single fixed delay, a partition's head is always
// the earliest-due record, so pausing on it never starves a later one. All
// pause/seek/timer mechanics live here; the consumer only calls Schedule.
type DelayScheduler struct {
	cl       *kgo.Client
	log      *log.Logger
	onResume func(topic string, partition int32)

	mu     sync.Mutex
	timers map[tp]*time.Timer
}

// NewDelayScheduler builds a scheduler over the consumer's client. onResume (may
// be nil) is called when a paused partition is resumed (e.g. to update a gauge).
func NewDelayScheduler(cl *kgo.Client, logger *log.Logger, onResume func(topic string, partition int32)) *DelayScheduler {
	if logger == nil {
		logger = log.Nop()
	}
	return &DelayScheduler{cl: cl, log: logger, onResume: onResume, timers: make(map[tp]*time.Timer)}
}

// Schedule pauses the record's partition, seeks back to the record so it is
// re-read later, and arranges to resume the partition at dueAt. It is idempotent
// per partition: a partition already scheduled is left as-is.
func (s *DelayScheduler) Schedule(pos RetryPosition, dueAt time.Time) {
	key := tp{pos.Topic, pos.Partition}

	s.mu.Lock()
	if _, exists := s.timers[key]; exists {
		s.mu.Unlock()
		return
	}
	// Reserve the slot before doing the pause so concurrent Schedules are no-ops.
	s.timers[key] = nil
	s.mu.Unlock()

	s.cl.PauseFetchPartitions(map[string][]int32{pos.Topic: {pos.Partition}})
	s.cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		pos.Topic: {pos.Partition: {Epoch: pos.Epoch, Offset: pos.Offset}},
	})

	d := time.Until(dueAt)
	if d < 0 {
		d = 0
	}
	timer := time.AfterFunc(d, func() { s.resume(key) })

	s.mu.Lock()
	// Revoke may have cleared the slot while we were pausing; honor that.
	if _, ok := s.timers[key]; ok {
		s.timers[key] = timer
	} else {
		timer.Stop()
	}
	s.mu.Unlock()
}

func (s *DelayScheduler) resume(key tp) {
	s.mu.Lock()
	delete(s.timers, key)
	s.mu.Unlock()
	s.cl.ResumeFetchPartitions(map[string][]int32{key.topic: {key.partition}})
	if s.onResume != nil {
		s.onResume(key.topic, key.partition)
	}
}

// Revoke cancels any pending resume for a partition being revoked. It clears the
// partition's paused state (harmless if no longer owned) and fires onResume so the
// paused-gauge is balanced — the increment that paired with the original Schedule
// would otherwise leak. It does not commit the uncommitted record (the new owner
// re-reads it).
func (s *DelayScheduler) Revoke(topic string, partition int32) {
	key := tp{topic, partition}
	s.mu.Lock()
	t, had := s.timers[key]
	if t != nil {
		t.Stop()
	}
	delete(s.timers, key)
	s.mu.Unlock()
	if !had {
		return
	}
	s.cl.ResumeFetchPartitions(map[string][]int32{topic: {partition}})
	if s.onResume != nil {
		s.onResume(topic, partition)
	}
	s.log.Debug("kafka retry scheduler: revoked paused partition",
		zap.String("topic", topic), zap.Int32("partition", partition))
}

// Stop cancels all pending resume timers (paused records stay uncommitted and are
// re-read after restart). It fires onResume for each so the paused gauge is reset.
func (s *DelayScheduler) Stop() {
	s.mu.Lock()
	keys := make([]tp, 0, len(s.timers))
	for k, t := range s.timers {
		if t != nil {
			t.Stop()
		}
		keys = append(keys, k)
	}
	s.timers = make(map[tp]*time.Timer)
	s.mu.Unlock()
	if s.onResume != nil {
		for _, k := range keys {
			s.onResume(k.topic, k.partition)
		}
	}
}

// Paused reports how many partitions are currently paused (for metrics/tests).
func (s *DelayScheduler) Paused() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.timers)
}
