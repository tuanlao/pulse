package dedup

import (
	"context"
	"testing"
	"time"
)

func TestLocalDeduper(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Mode = "local"
	cfg.ApplyDefaults()

	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	seen, _ := d.Seen(ctx, "id1")
	if seen {
		t.Fatal("Seen(id1) = true before Mark")
	}
	if err := d.Mark(ctx, "id1", time.Minute); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	seen, _ = d.Seen(ctx, "id1")
	if !seen {
		t.Fatal("Seen(id1) = false after Mark")
	}
	seen, _ = d.Seen(ctx, "id2")
	if seen {
		t.Fatal("Seen(id2) = true (never marked)")
	}
}

func TestDisabledReturnsNil(t *testing.T) {
	d, err := New(DefaultConfig(), nil) // Enabled=false
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if d != nil {
		t.Fatal("disabled dedup should return a nil Deduper")
	}
}

func TestRedisModeRequiresClient(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Mode = "redis"
	if _, err := New(cfg, nil); err == nil {
		t.Fatal("redis mode without a client should fail fast")
	}
}
