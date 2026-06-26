//go:build integration

// These tests require a live redis (>= 6, RESP3) at REDIS_ADDR (default
// localhost:6379). Run with: go test -tags=integration ./pkg/redis/...
package redis

import (
	"context"
	"os"
	"testing"
	"time"
)

func addr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

func TestIntegration_ClientSideCache(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithAddresses(addr()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start (ping): %v", err)
	}
	defer func() { _ = c.Stop(ctx) }()

	const key = "pulse:redis:test:csc"
	if err := c.Do(ctx, c.B().Set().Key(key).Value("v1").Build()).Error(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	// First DoCache populates the client-side cache (miss); the second is served
	// locally (hit).
	first := c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute)
	if v, _ := first.ToString(); v != "v1" {
		t.Fatalf("first GET = %q", v)
	}
	if first.IsCacheHit() {
		t.Fatalf("first DoCache should be a miss")
	}
	second := c.DoCache(ctx, c.B().Get().Key(key).Cache(), time.Minute)
	if !second.IsCacheHit() {
		t.Fatalf("second DoCache should be a client-side cache hit")
	}
}

func TestIntegration_Locker(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithAddresses(addr()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start (ping): %v", err)
	}
	defer func() { _ = c.Stop(ctx) }()

	locker := NewLocker(c.Client, WithLockKeyPrefix("pulse:redis:test:lock"), WithLockTTL(5*time.Second))

	lk, err := locker.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("first Lock should succeed: %v", err)
	}

	// A second acquisition of the same key must fail (mutual exclusion).
	if _, err := locker.Lock(ctx, "job"); err != ErrLockNotAcquired {
		t.Fatalf("second Lock should be ErrLockNotAcquired, got %v", err)
	}

	// The owner still holds it.
	if ok, err := lk.Valid(ctx); err != nil || !ok {
		t.Fatalf("Valid = %v, %v; want true, nil", ok, err)
	}

	// Extend and release (owner-checked).
	if err := lk.Extend(ctx); err != nil {
		t.Fatalf("Extend: %v", err)
	}
	if err := lk.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// Unlocking again is a no-op for ownership (already released).
	if err := lk.Unlock(ctx); err != ErrLockNotHeld {
		t.Fatalf("second Unlock should be ErrLockNotHeld, got %v", err)
	}

	// After release the lock is free again.
	lk2, err := locker.Lock(ctx, "job")
	if err != nil {
		t.Fatalf("re-Lock after release should succeed: %v", err)
	}
	_ = lk2.Unlock(ctx)
}
