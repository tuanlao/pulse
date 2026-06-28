//go:build integration

// Additional integration tests for pkg/redis against a live redis (>= 6, RESP3)
// at REDIS_ADDR: client-side cache invalidation, broadcast (BCAST) prefix
// tracking, and Locker TTL/retry/contention semantics.
package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func mustRedis(t *testing.T, opts ...Option) *Client {
	t.Helper()
	opts = append([]Option{WithAddresses(addr())}, opts...)
	c, err := New(DefaultConfig(), Deps{}, opts...)
	if err != nil {
		t.Fatalf("redis.New: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("redis.Start (is redis at %s?): %v", addr(), err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func waitCond(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", msg)
}

func cachedGet(ctx context.Context, c *Client, key string) string {
	v, _ := c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute).ToString()
	return v
}

// TestIntegration_ClientSideCache_InvalidationOnMutation: a value cached locally
// is invalidated when another client mutates the key, so the next read refetches.
func TestIntegration_ClientSideCache_InvalidationOnMutation(t *testing.T) {
	ctx := context.Background()
	c := mustRedis(t)  // caching client
	c2 := mustRedis(t) // mutator
	key := "pulse:redis:test:cscinv:" + uuid.NewString()

	if err := c2.Do(ctx, c2.B().Set().Key(key).Value("v1").Build()).Error(); err != nil {
		t.Fatalf("SET v1: %v", err)
	}

	first := c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute)
	if v, _ := first.ToString(); v != "v1" {
		t.Fatalf("first cached GET = %q, want v1", v)
	}
	if first.IsCacheHit() {
		t.Fatal("first DoCache should be a miss")
	}
	if !c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute).IsCacheHit() {
		t.Fatal("second DoCache should be a local cache hit")
	}

	// Mutate from another client -> the cached entry must be invalidated.
	if err := c2.Do(ctx, c2.B().Set().Key(key).Value("v2").Build()).Error(); err != nil {
		t.Fatalf("SET v2: %v", err)
	}
	waitCond(t, 10*time.Second, func() bool { return cachedGet(ctx, c, key) == "v2" },
		"cache invalidated and refetched v2")
}

// TestIntegration_ClientSideCache_Broadcast: broadcast (BCAST) prefix tracking
// invalidates cached reads for keys under the tracked prefix.
func TestIntegration_ClientSideCache_Broadcast(t *testing.T) {
	ctx := context.Background()
	prefix := "pulse:redis:test:bcast:" + uuid.NewString() + ":"
	c := mustRedis(t, WithClientCache(true), WithBroadcast(prefix))
	c2 := mustRedis(t)
	key := prefix + "k"

	if err := c2.Do(ctx, c2.B().Set().Key(key).Value("v1").Build()).Error(); err != nil {
		t.Fatalf("SET v1: %v", err)
	}
	if v := cachedGet(ctx, c, key); v != "v1" {
		t.Fatalf("first cached GET = %q, want v1", v)
	}
	if !c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute).IsCacheHit() {
		t.Fatal("second DoCache should be a local cache hit (broadcast tracking)")
	}

	if err := c2.Do(ctx, c2.B().Set().Key(key).Value("v2").Build()).Error(); err != nil {
		t.Fatalf("SET v2: %v", err)
	}
	waitCond(t, 10*time.Second, func() bool { return cachedGet(ctx, c, key) == "v2" },
		"broadcast invalidation refetched v2")
}

// TestIntegration_Locker_TTLExpiry: a lock whose TTL lapses is no longer valid and
// can be acquired by another contender.
func TestIntegration_Locker_TTLExpiry(t *testing.T) {
	ctx := context.Background()
	c := mustRedis(t)
	locker := NewLocker(c.Client,
		WithLockKeyPrefix("pulse:redis:test:ttl:"+uuid.NewString()),
		WithLockTTL(1*time.Second))

	lk, err := locker.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if _, err := locker.Lock(ctx, "job"); err != ErrLockNotAcquired {
		t.Fatalf("contended Lock = %v, want ErrLockNotAcquired", err)
	}

	time.Sleep(1500 * time.Millisecond) // let the lease lapse

	if ok, _ := lk.Valid(ctx); ok {
		t.Fatal("lock should be invalid after its TTL lapsed")
	}
	lk2, err := locker.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("re-acquire after TTL expiry should succeed, got %v", err)
	}
	_ = lk2.Unlock(ctx)
}

// TestIntegration_Locker_RetriesAcquireAfterRelease: a locker configured with
// Tries>1 keeps retrying and acquires once the holder releases.
func TestIntegration_Locker_RetriesAcquireAfterRelease(t *testing.T) {
	ctx := context.Background()
	c := mustRedis(t)
	prefix := "pulse:redis:test:retry:" + uuid.NewString()

	holder := NewLocker(c.Client, WithLockKeyPrefix(prefix), WithLockTTL(10*time.Second))
	lk1, err := holder.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("holder Lock: %v", err)
	}
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = lk1.Unlock(ctx)
	}()

	retrier := NewLocker(c.Client, WithLockKeyPrefix(prefix),
		WithLockTTL(10*time.Second), WithLockTries(20), WithLockRetryDelay(100*time.Millisecond))
	lk2, err := retrier.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("retrying Lock should acquire after the holder releases, got %v", err)
	}
	_ = lk2.Unlock(ctx)
}

// TestIntegration_Locker_ConcurrentOneWinner: many contenders racing for the same
// key, exactly one acquires it.
func TestIntegration_Locker_ConcurrentOneWinner(t *testing.T) {
	ctx := context.Background()
	c := mustRedis(t)
	prefix := "pulse:redis:test:race:" + uuid.NewString()
	const n = 8

	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			locker := NewLocker(c.Client, WithLockKeyPrefix(prefix), WithLockTTL(5*time.Second))
			if _, err := locker.Lock(ctx, "job"); err == nil {
				wins.Add(1) // winner holds the lock until it expires
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("expected exactly one lock winner, got %d", got)
	}
}
