package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
)

// Sentinel errors returned by the locker.
var (
	// ErrLockNotAcquired means the key is already held by another owner (and all
	// retries were exhausted). Callers that want "run on exactly one pod" treat
	// this as "skip".
	ErrLockNotAcquired = errors.New("redis: lock not acquired")
	// ErrLockNotHeld means Unlock/Extend ran but this owner no longer holds the
	// lock (it expired or was taken over) — the compare-and-act check failed.
	ErrLockNotHeld = errors.New("redis: lock not held by this owner")
)

// Lua scripts implement the safe compare-and-act half of the mutex: release and
// extend only when the caller still owns the lock (value == its token), so one
// owner can never drop or prolong another's lock. rueidis caches the SHA and
// uses EVALSHA with an EVAL fallback.
var (
	unlockScript = rueidis.NewLuaScript(
		`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`)
	extendScript = rueidis.NewLuaScript(
		`if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`)
)

// LockerConfig configures the distributed mutex.
type LockerConfig struct {
	// KeyPrefix namespaces lock keys (joined to the lock name with ":"). Default
	// "pulse:lock".
	KeyPrefix string
	// TTL is the lock's auto-expiry — the safety net that releases a lock whose
	// owner crashed without calling Unlock. It MUST exceed the critical section's
	// duration (or refresh it with Lock.Extend). Default 30s.
	TTL time.Duration
	// Tries is how many times Lock attempts acquisition before returning
	// ErrLockNotAcquired (1 = single attempt, skip if held). Default 1.
	Tries int
	// RetryDelay is the wait between attempts when Tries > 1. Default 100ms.
	RetryDelay time.Duration
}

// DefaultLockerConfig returns sensible defaults.
func DefaultLockerConfig() LockerConfig {
	return LockerConfig{
		KeyPrefix:  "pulse:lock",
		TTL:        30 * time.Second,
		Tries:      1,
		RetryDelay: 100 * time.Millisecond,
	}
}

// LockerOption overrides LockerConfig.
type LockerOption func(*LockerConfig)

// WithLockKeyPrefix sets the key namespace.
func WithLockKeyPrefix(prefix string) LockerOption {
	return func(c *LockerConfig) { c.KeyPrefix = prefix }
}

// WithLockTTL sets the lock auto-expiry.
func WithLockTTL(d time.Duration) LockerOption { return func(c *LockerConfig) { c.TTL = d } }

// WithLockTries sets the number of acquisition attempts.
func WithLockTries(n int) LockerOption { return func(c *LockerConfig) { c.Tries = n } }

// WithLockRetryDelay sets the delay between acquisition attempts.
func WithLockRetryDelay(d time.Duration) LockerOption {
	return func(c *LockerConfig) { c.RetryDelay = d }
}

// Locker is a redis distributed mutex built on rueidis. Acquisition is an atomic
// SET NX PX; release/extend are owner-checked Lua scripts. It holds a
// rueidis.Client but does not own it (the caller closes it).
type Locker struct {
	client rueidis.Client
	cfg    LockerConfig
}

// NewLocker builds a Locker over the given rueidis client.
func NewLocker(client rueidis.Client, opts ...LockerOption) *Locker {
	cfg := DefaultLockerConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	d := DefaultLockerConfig()
	if cfg.TTL <= 0 {
		cfg.TTL = d.TTL
	}
	if cfg.Tries <= 0 {
		cfg.Tries = d.Tries
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = d.RetryDelay
	}
	return &Locker{client: client, cfg: cfg}
}

func (l *Locker) key(name string) string {
	if l.cfg.KeyPrefix == "" {
		return name
	}
	return l.cfg.KeyPrefix + ":" + name
}

// Lock acquires the named lock, returning a *Lock the caller must Unlock. It
// returns ErrLockNotAcquired when the lock is held by another owner (after
// Tries attempts), or the context error if ctx is cancelled while retrying.
func (l *Locker) Lock(ctx context.Context, name string) (*Lock, error) {
	if l.client == nil {
		return nil, errors.New("redis: locker has no client")
	}
	key := l.key(name)
	token := uuid.NewString()

	for attempt := 0; attempt < l.cfg.Tries; attempt++ {
		if attempt > 0 {
			// time.NewTimer + Stop instead of time.After so a cancelled ctx does
			// not leak the underlying timer until RetryDelay elapses.
			t := time.NewTimer(l.cfg.RetryDelay)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, ctx.Err()
			case <-t.C:
			}
		}
		err := l.client.Do(ctx, l.client.B().Set().Key(key).Value(token).Nx().Px(l.cfg.TTL).Build()).Error()
		if err == nil {
			return &Lock{locker: l, key: key, token: token}, nil
		}
		if rueidis.IsRedisNil(err) {
			continue // held by another owner — retry or give up
		}
		return nil, fmt.Errorf("redis: acquire lock %q: %w", key, err)
	}
	return nil, ErrLockNotAcquired
}

// Lock is an acquired lock. It is safe for the owning goroutine to Unlock,
// Extend or check Valid; it carries a unique token so those operations only
// affect this owner's lock.
type Lock struct {
	locker *Locker
	key    string
	token  string
}

// Key returns the fully-qualified redis key (prefix + name).
func (lk *Lock) Key() string { return lk.key }

// Unlock releases the lock, but only if this owner still holds it (Lua
// compare-and-delete). It returns ErrLockNotHeld if the lock had already
// expired or been taken over.
func (lk *Lock) Unlock(ctx context.Context) error {
	n, err := unlockScript.Exec(ctx, lk.locker.client, []string{lk.key}, []string{lk.token}).AsInt64()
	if err != nil {
		return fmt.Errorf("redis: unlock %q: %w", lk.key, err)
	}
	if n == 0 {
		return ErrLockNotHeld
	}
	return nil
}

// Extend resets the lock's TTL to the configured TTL, but only if this owner
// still holds it (Lua compare-and-pexpire). Use it as a watchdog for critical
// sections that may outlast the TTL. Returns ErrLockNotHeld if no longer owned.
func (lk *Lock) Extend(ctx context.Context) error {
	ms := strconv.FormatInt(lk.locker.cfg.TTL.Milliseconds(), 10)
	n, err := extendScript.Exec(ctx, lk.locker.client, []string{lk.key}, []string{lk.token, ms}).AsInt64()
	if err != nil {
		return fmt.Errorf("redis: extend %q: %w", lk.key, err)
	}
	if n == 0 {
		return ErrLockNotHeld
	}
	return nil
}

// Valid reports whether this owner still holds the lock (the stored value still
// equals its token). Useful to re-check ownership before doing critical work.
func (lk *Lock) Valid(ctx context.Context) (bool, error) {
	v, err := lk.locker.client.Do(ctx, lk.locker.client.B().Get().Key(lk.key).Build()).ToString()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("redis: check lock %q: %w", lk.key, err)
	}
	return v == lk.token, nil
}
