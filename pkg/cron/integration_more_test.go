//go:build integration

package cron

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	predis "github.com/tuanlao/pulse/pkg/redis"
)

func mustCronRedis(t *testing.T) *predis.Client {
	t.Helper()
	c, err := predis.New(predis.DefaultConfig(), predis.Deps{}, predis.WithAddresses(lockRedisAddr()))
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("redis.Start (is redis at %s?): %v", lockRedisAddr(), err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

// perKeyConc tracks the peak concurrency observed per logical key (job name).
type perKeyConc struct {
	mu    sync.Mutex
	cur   map[string]int
	max   map[string]int
	total map[string]int
}

func newPerKeyConc() *perKeyConc {
	return &perKeyConc{cur: map[string]int{}, max: map[string]int{}, total: map[string]int{}}
}

func (p *perKeyConc) enter(k string) {
	p.mu.Lock()
	p.cur[k]++
	if p.cur[k] > p.max[k] {
		p.max[k] = p.cur[k]
	}
	p.total[k]++
	p.mu.Unlock()
}

func (p *perKeyConc) leave(k string) {
	p.mu.Lock()
	p.cur[k]--
	p.mu.Unlock()
}

func (p *perKeyConc) snapshot(k string) (max, total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.max[k], p.total[k]
}

// TestIntegration_DistributedLock_SharedRedisClient: the distributed lock works
// when the rueidis client is supplied via Deps.RedisClient (instead of
// Lock.Redis.Address) — two pods still see single-pod-per-run.
func TestIntegration_DistributedLock_SharedRedisClient(t *testing.T) {
	rc := mustCronRedis(t)
	keyPrefix := "pulse:it:cron:shared:" + uuid.NewString()
	conc := newPerKeyConc()

	job := func(context.Context) error {
		conc.enter("tick")
		time.Sleep(250 * time.Millisecond)
		conc.leave("tick")
		return nil
	}
	newPod := func() *Scheduler {
		cfg := DefaultConfig()
		cfg.Jobs = map[string]JobConfig{"tick": {Enabled: true, Every: 200 * time.Millisecond}}
		cfg.Lock.Enabled = true
		cfg.Lock.KeyPrefix = keyPrefix
		cfg.Lock.TTL = 5 * time.Second
		s, err := New(cfg, Deps{RedisClient: rc})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s.Register("tick", job)
		return s
	}

	a, b := newPod(), newPod()
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("podA.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("podB.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	time.Sleep(4 * time.Second)
	max, total := conc.snapshot("tick")
	if max != 1 {
		t.Fatalf("max concurrent runs = %d, want 1 (shared-client distributed lock)", max)
	}
	if total < 3 {
		t.Fatalf("job ran %d times, want >= 3", total)
	}
}

// TestIntegration_DistributedLock_MultipleJobsIndependent: two jobs locked
// independently — each is single-pod-per-run, and they do not block each other
// (different lock keys), so both keep firing.
func TestIntegration_DistributedLock_MultipleJobsIndependent(t *testing.T) {
	rc := mustCronRedis(t)
	keyPrefix := "pulse:it:cron:multi:" + uuid.NewString()
	conc := newPerKeyConc()

	mkJob := func(name string) JobFunc {
		return func(context.Context) error {
			conc.enter(name)
			time.Sleep(250 * time.Millisecond)
			conc.leave(name)
			return nil
		}
	}
	newPod := func() *Scheduler {
		cfg := DefaultConfig()
		cfg.Jobs = map[string]JobConfig{
			"jobA": {Enabled: true, Every: 200 * time.Millisecond},
			"jobB": {Enabled: true, Every: 200 * time.Millisecond},
		}
		cfg.Lock.Enabled = true
		cfg.Lock.KeyPrefix = keyPrefix
		cfg.Lock.TTL = 5 * time.Second
		s, err := New(cfg, Deps{RedisClient: rc})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		s.Register("jobA", mkJob("jobA"))
		s.Register("jobB", mkJob("jobB"))
		return s
	}

	a, b := newPod(), newPod()
	if err := a.Start(context.Background()); err != nil {
		t.Fatalf("podA.Start: %v", err)
	}
	t.Cleanup(func() { _ = a.Stop(context.Background()) })
	if err := b.Start(context.Background()); err != nil {
		t.Fatalf("podB.Start: %v", err)
	}
	t.Cleanup(func() { _ = b.Stop(context.Background()) })

	time.Sleep(4 * time.Second)
	for _, name := range []string{"jobA", "jobB"} {
		max, total := conc.snapshot(name)
		if max != 1 {
			t.Fatalf("%s max concurrent runs = %d, want 1 (single-pod-per-run)", name, max)
		}
		if total < 3 {
			t.Fatalf("%s ran %d times, want >= 3 (jobs must not block each other)", name, total)
		}
	}
}
