//go:build integration

// Integration test for the cron distributed lock against a REAL redis. It models
// two pods (two schedulers) running the SAME job, contending on the same redis
// lock key, and asserts the lock enforces mutual exclusion: the job never runs on
// both pods at the same time (single-pod-per-run).
//
// It needs a live redis (>= 6, RESP3). Run with the docker-compose stack:
//
//	make infra-up
//	go test -race -tags=integration ./pkg/cron/... -run TestIntegration -v
//
// The redis address comes from REDIS_ADDR (default localhost:6379).
package cron

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func lockRedisAddr() string {
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

func TestIntegration_DistributedLock_SinglePodPerRun(t *testing.T) {
	// Shared key prefix => both schedulers contend on the SAME lock for the job.
	keyPrefix := "pulse:it:cron:" + uuid.NewString()

	// Shared observation state across both "pods".
	var mu sync.Mutex
	var current, max, total int
	job := func(context.Context) error {
		mu.Lock()
		current++
		if current > max {
			max = current
		}
		total++
		mu.Unlock()

		// Hold longer than the tick period so, WITHOUT the lock, the two pods would
		// overlap (max==2). With the lock, the loser skips and max stays 1.
		time.Sleep(250 * time.Millisecond)

		mu.Lock()
		current--
		mu.Unlock()
		return nil
	}

	newPod := func() *Scheduler {
		cfg := DefaultConfig()
		cfg.Jobs = map[string]JobConfig{
			"tick": {Enabled: true, Every: 200 * time.Millisecond},
		}
		cfg.Lock.Enabled = true
		cfg.Lock.Redis.Address = lockRedisAddr()
		cfg.Lock.KeyPrefix = keyPrefix
		cfg.Lock.TTL = 5 * time.Second // auto-expire a stuck lock well within the test

		s, err := New(cfg, Deps{})
		if err != nil {
			t.Fatalf("New scheduler: %v", err)
		}
		s.Register("tick", job)
		return s
	}

	podA, podB := newPod(), newPod()

	ctx := context.Background()
	if err := podA.Start(ctx); err != nil {
		t.Fatalf("podA.Start (is redis reachable at %s?): %v", lockRedisAddr(), err)
	}
	t.Cleanup(func() { _ = podA.Stop(context.Background()) })
	if err := podB.Start(ctx); err != nil {
		t.Fatalf("podB.Start: %v", err)
	}
	t.Cleanup(func() { _ = podB.Stop(context.Background()) })

	// Let the two pods contend for a few seconds.
	time.Sleep(4 * time.Second)

	mu.Lock()
	gotMax, gotTotal := max, total
	mu.Unlock()

	if gotMax != 1 {
		t.Fatalf("max concurrent executions = %d, want 1 (distributed lock must enforce mutual exclusion)", gotMax)
	}
	if gotTotal < 3 {
		t.Fatalf("job ran %d times, want >= 3 (it should keep firing under the lock)", gotTotal)
	}
	t.Logf("distributed lock held: %d runs, max concurrency %d", gotTotal, gotMax)
}
