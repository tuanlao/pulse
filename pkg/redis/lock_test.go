package redis

import (
	"testing"
	"time"
)

func TestDefaultLockerConfig(t *testing.T) {
	c := DefaultLockerConfig()
	if c.KeyPrefix != "pulse:lock" || c.TTL != 30*time.Second || c.Tries != 1 || c.RetryDelay != 100*time.Millisecond {
		t.Fatalf("unexpected default locker config: %+v", c)
	}
}

func TestNewLocker_DefaultsAndOptions(t *testing.T) {
	// Zero/negative values fall back to defaults.
	l := NewLocker(nil, WithLockTTL(0), WithLockTries(0), WithLockRetryDelay(-1))
	if l.cfg.TTL != 30*time.Second || l.cfg.Tries != 1 || l.cfg.RetryDelay != 100*time.Millisecond {
		t.Fatalf("defaults not applied: %+v", l.cfg)
	}

	// Options override.
	l = NewLocker(nil,
		WithLockKeyPrefix("svc:lock"),
		WithLockTTL(5*time.Second),
		WithLockTries(3),
		WithLockRetryDelay(50*time.Millisecond),
	)
	if l.cfg.KeyPrefix != "svc:lock" || l.cfg.TTL != 5*time.Second || l.cfg.Tries != 3 || l.cfg.RetryDelay != 50*time.Millisecond {
		t.Fatalf("options not applied: %+v", l.cfg)
	}
}

func TestLockerKey(t *testing.T) {
	if got := NewLocker(nil).key("job"); got != "pulse:lock:job" {
		t.Fatalf("key = %q, want pulse:lock:job", got)
	}
	if got := NewLocker(nil, WithLockKeyPrefix("")).key("job"); got != "job" {
		t.Fatalf("empty prefix key = %q, want job", got)
	}
}
