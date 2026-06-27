//go:build integration

// These tests need a live redis at localhost:6379. Run with:
//
//	go test -race -tags integration ./pkg/snowflake/...
package snowflake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func redisCfg(t *testing.T) Config {
	t.Helper()
	cfg := DefaultConfig()
	cfg.WorkerID.Strategy = StrategyRedis
	cfg.WorkerID.Redis.Redis.Address = "localhost:6379"
	// Isolate each test run so a crashed prior run can't leak slots into it.
	cfg.WorkerID.Redis.KeyPrefix = "pulse:test:snowflake:" + uuid.NewString()
	cfg.WorkerID.Redis.TTL = 2 * time.Second
	return cfg
}

func TestIntegration_RedisDistinctSlots(t *testing.T) {
	cfg := redisCfg(t)
	cfg.NodeBits = 4 // 16 slots

	g1, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	g2, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}

	ctx := context.Background()
	if err := g1.Start(ctx); err != nil {
		t.Fatalf("start g1: %v", err)
	}
	t.Cleanup(func() { _ = g1.Stop(context.Background()) })
	if err := g2.Start(ctx); err != nil {
		t.Fatalf("start g2: %v", err)
	}
	t.Cleanup(func() { _ = g2.Stop(context.Background()) })

	if g1.WorkerID() == g2.WorkerID() {
		t.Fatalf("expected distinct worker ids, both got %d", g1.WorkerID())
	}
	if g1.Node(g1.Generate()) != g1.WorkerID() {
		t.Fatalf("g1 generated id does not carry its worker id")
	}
}

func TestIntegration_RedisPoolExhausted(t *testing.T) {
	cfg := redisCfg(t)
	cfg.NodeBits = 1 // 2 slots only

	ctx := context.Background()
	g1, _ := New(cfg, Deps{})
	g2, _ := New(cfg, Deps{})
	g3, _ := New(cfg, Deps{})

	if err := g1.Start(ctx); err != nil {
		t.Fatalf("start g1: %v", err)
	}
	t.Cleanup(func() { _ = g1.Stop(context.Background()) })
	if err := g2.Start(ctx); err != nil {
		t.Fatalf("start g2: %v", err)
	}
	t.Cleanup(func() { _ = g2.Stop(context.Background()) })

	if err := g3.Start(ctx); !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected ErrPoolExhausted for the 3rd contender, got %v", err)
	}
}

func TestIntegration_RedisStopReleasesSlot(t *testing.T) {
	cfg := redisCfg(t)
	cfg.NodeBits = 1 // 2 slots only

	ctx := context.Background()
	g1, _ := New(cfg, Deps{})
	g2, _ := New(cfg, Deps{})
	if err := g1.Start(ctx); err != nil {
		t.Fatalf("start g1: %v", err)
	}
	if err := g2.Start(ctx); err != nil {
		t.Fatalf("start g2: %v", err)
	}
	t.Cleanup(func() { _ = g2.Stop(context.Background()) })

	// Pool is full; releasing g1's slot lets a new generator reclaim it.
	if err := g1.Stop(ctx); err != nil {
		t.Fatalf("stop g1: %v", err)
	}

	g3, _ := New(cfg, Deps{})
	if err := g3.Start(ctx); err != nil {
		t.Fatalf("expected g3 to reclaim the freed slot, got %v", err)
	}
	t.Cleanup(func() { _ = g3.Stop(context.Background()) })
}
