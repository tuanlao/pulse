//go:build integration

package snowflake

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	predis "github.com/tuanlao/pulse/pkg/redis"
)

// TestIntegration_RedisLeaseRenewalKeepsSlot: the background renewal keeps the
// slot lease alive well past its TTL, so the generator keeps the same worker id
// and keeps minting ids.
func TestIntegration_RedisLeaseRenewalKeepsSlot(t *testing.T) {
	cfg := redisCfg(t) // TTL = 2s, renew ~ TTL/3
	cfg.NodeBits = 4

	g, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })

	wid := g.WorkerID()
	// Run for several TTL windows; the lease must be renewed (not expire).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := g.TryGenerate(); err != nil {
			t.Fatalf("TryGenerate failed mid-run (lease lost?): %v", err)
		}
		if g.WorkerID() != wid {
			t.Fatalf("worker id changed from %d to %d (slot lost)", wid, g.WorkerID())
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestIntegration_RedisConcurrentUniqueness: several generators with distinct
// slots, each minting concurrently, never collide.
func TestIntegration_RedisConcurrentUniqueness(t *testing.T) {
	cfg := redisCfg(t)
	cfg.NodeBits = 4 // 16 slots

	const gens = 3
	const goroutines = 20
	const perGoroutine = 50

	ctx := context.Background()
	generators := make([]*Generator, gens)
	for i := range generators {
		g, err := New(cfg, Deps{})
		if err != nil {
			t.Fatalf("New g%d: %v", i, err)
		}
		if err := g.Start(ctx); err != nil {
			t.Fatalf("Start g%d: %v", i, err)
		}
		t.Cleanup(func() { _ = g.Stop(context.Background()) })
		generators[i] = g
	}

	var mu sync.Mutex
	seen := map[ID]bool{}
	var dup int
	var wg sync.WaitGroup
	for _, g := range generators {
		for w := 0; w < goroutines; w++ {
			wg.Add(1)
			go func(g *Generator) {
				defer wg.Done()
				for k := 0; k < perGoroutine; k++ {
					id := g.Generate()
					mu.Lock()
					if seen[id] {
						dup++
					}
					seen[id] = true
					mu.Unlock()
				}
			}(g)
		}
	}
	wg.Wait()

	if dup != 0 {
		t.Fatalf("found %d duplicate ids across %d generators", dup, gens)
	}
	if want := gens * goroutines * perGoroutine; len(seen) != want {
		t.Fatalf("minted %d distinct ids, want %d", len(seen), want)
	}
}

// TestIntegration_RedisFencingOnLeaseLoss: if the slot lease is taken over by
// someone else, the generator detects the loss on its next renewal and fences
// (refuses to mint) instead of risking a duplicate worker id.
func TestIntegration_RedisFencingOnLeaseLoss(t *testing.T) {
	cfg := redisCfg(t) // TTL 2s, renew ~666ms
	cfg.NodeBits = 1   // 2 slots only, so a fenced generator cannot re-acquire a free one

	ctx := context.Background()
	g, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New g: %v", err)
	}
	if err := g.Start(ctx); err != nil {
		t.Fatalf("Start g: %v", err)
	}
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	// A second generator fills the pool so no free slot remains to re-acquire.
	g2, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	if err := g2.Start(ctx); err != nil {
		t.Fatalf("Start g2: %v", err)
	}
	t.Cleanup(func() { _ = g2.Stop(context.Background()) })

	if _, err := g.TryGenerate(); err != nil {
		t.Fatalf("generator should mint before the lease is lost: %v", err)
	}

	// Take over the slot key so the generator's renewal can neither extend nor
	// re-acquire it — forcing it to fence.
	rc, err := predis.New(predis.DefaultConfig(), predis.Deps{}, predis.WithAddresses("localhost:6379"))
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	if err := rc.Start(ctx); err != nil {
		t.Fatalf("redis.Start: %v", err)
	}
	t.Cleanup(func() { _ = rc.Stop(context.Background()) })

	keys, err := rc.Do(ctx, rc.B().Keys().Pattern(cfg.WorkerID.Redis.KeyPrefix+"*").Build()).AsStrSlice()
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("no slot-lease key found for the generator")
	}
	for _, k := range keys {
		if err := rc.Do(ctx, rc.B().Set().Key(k).Value("stolen").Px(10*time.Second).Build()).Error(); err != nil {
			t.Fatalf("overwrite lease key %q: %v", k, err)
		}
	}

	// Within a couple of renewal cycles the generator must fence.
	var fenced bool
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := g.TryGenerate(); errors.Is(err, ErrLeaseLost) {
			fenced = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !fenced {
		t.Fatal("generator did not fence after its slot lease was stolen")
	}
}

// TestIntegration_StatefulSetStrategy: the statefulset strategy derives the worker
// id from the pod ordinal; a pod name without an ordinal fails fast. (No redis.)
func TestIntegration_StatefulSetStrategy(t *testing.T) {
	t.Setenv("IT_POD_NAME", "billing-7")
	g, err := New(DefaultConfig(), Deps{}, WithStatefulSetStrategy(), WithPodNameEnv("IT_POD_NAME"))
	if err != nil {
		t.Fatalf("New (statefulset): %v", err)
	}
	if g.WorkerID() != 7 {
		t.Fatalf("worker id = %d, want 7 (pod ordinal)", g.WorkerID())
	}

	t.Setenv("IT_POD_NAME", "billing") // no ordinal suffix
	if _, err := New(DefaultConfig(), Deps{}, WithStatefulSetStrategy(), WithPodNameEnv("IT_POD_NAME")); !errors.Is(err, ErrNotStatefulSet) {
		t.Fatalf("expected ErrNotStatefulSet for a pod name without an ordinal, got %v", err)
	}
}
