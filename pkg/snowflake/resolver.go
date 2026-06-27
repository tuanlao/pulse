package snowflake

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	pulseredis "github.com/tuanlao/pulse/pkg/redis"
	"github.com/tuanlao/pulse/pkg/tracing"
	"go.uber.org/zap"
)

// Resolution errors.
var (
	// ErrNotStatefulSet means the pod name has no ordinal suffix — the pod is part
	// of a Deployment (random suffix), not a StatefulSet, so no worker id can be
	// derived. Use the static or redis strategy instead.
	ErrNotStatefulSet = errors.New("snowflake: pod name has no ordinal suffix (running as a Deployment? use the static or redis strategy)")
	// ErrWorkerIDOutOfRange means the resolved worker id does not fit in NodeBits.
	ErrWorkerIDOutOfRange = errors.New("snowflake: worker id out of range for node_bits")
	// ErrPoolExhausted means every redis worker-id slot is already held by another
	// pod (increase node_bits or reduce replicas).
	ErrPoolExhausted = errors.New("snowflake: all worker-id slots are in use")
)

// workerIDResolver acquires a worker (node) id in [0, maxNode]. The static and
// statefulset resolvers are pure (resolve in New); the redis resolver does I/O
// and returns a *redisLease the generator holds for renewal/fencing (resolve in
// Start).
type workerIDResolver interface {
	resolve(ctx context.Context, maxNode int64) (node int64, lease *redisLease, err error)
}

// staticResolver returns a fixed, configured worker id.
type staticResolver struct{ id int64 }

func (r staticResolver) resolve(_ context.Context, maxNode int64) (int64, *redisLease, error) {
	if r.id < 0 || r.id > maxNode {
		return 0, nil, fmt.Errorf("%w: static id %d not in [0,%d]", ErrWorkerIDOutOfRange, r.id, maxNode)
	}
	return r.id, nil, nil
}

// statefulSetResolver derives the worker id from the StatefulSet pod ordinal.
type statefulSetResolver struct{ podNameEnv string }

func (r statefulSetResolver) resolve(_ context.Context, maxNode int64) (int64, *redisLease, error) {
	name := os.Getenv(r.podNameEnv)
	if name == "" {
		h, err := os.Hostname()
		if err != nil {
			return 0, nil, fmt.Errorf("snowflake: read hostname for statefulset ordinal: %w", err)
		}
		name = h
	}
	ord, err := parseOrdinal(name)
	if err != nil {
		return 0, nil, err
	}
	if int64(ord) > maxNode {
		return 0, nil, fmt.Errorf("%w: ordinal %d from pod %q exceeds max node id %d (increase node_bits)",
			ErrWorkerIDOutOfRange, ord, name, maxNode)
	}
	return int64(ord), nil, nil
}

// parseOrdinal extracts the trailing StatefulSet ordinal from a pod name
// ("web-3" -> 3, "my-app-12" -> 12). It returns ErrNotStatefulSet when the name
// has no "-" or the segment after the last "-" is not a non-negative integer (the
// Deployment case, e.g. "web-5d4b9c-xk2lp").
func parseOrdinal(podName string) (int, error) {
	i := strings.LastIndex(podName, "-")
	if i < 0 || i == len(podName)-1 {
		return 0, fmt.Errorf("%w: %q", ErrNotStatefulSet, podName)
	}
	ord, err := strconv.Atoi(podName[i+1:])
	if err != nil || ord < 0 {
		return 0, fmt.Errorf("%w: %q", ErrNotStatefulSet, podName)
	}
	return ord, nil
}

// redisResolver acquires a slot via a redisLease built ahead of time (in New).
type redisResolver struct{ lease *redisLease }

func (r redisResolver) resolve(ctx context.Context, _ int64) (int64, *redisLease, error) {
	if err := r.lease.acquire(ctx); err != nil {
		return 0, nil, err
	}
	return r.lease.node(), r.lease, nil
}

// redisLease holds one contended worker-id slot: it claims a free slot, keeps the
// lease alive with a background renewal goroutine, and fences (refuses to
// generate) if the lease is lost, re-acquiring a slot when it can. node and
// fenced are atomics for lock-free hot-path reads; lock/lastOK are guarded by mu.
type redisLease struct {
	locker        *pulseredis.Locker
	poolSize      int
	ttl           time.Duration
	renewInterval time.Duration
	log           *log.Logger
	metrics       *Metrics

	mu     sync.Mutex
	lock   *pulseredis.Lock
	lastOK time.Time

	nodeID atomic.Int64
	fenced atomic.Bool

	done chan struct{}
	wg   sync.WaitGroup
}

func (rl *redisLease) node() int64    { return rl.nodeID.Load() }
func (rl *redisLease) isFenced() bool { return rl.fenced.Load() }

// acquire claims an initial slot and starts the renewal goroutine.
func (rl *redisLease) acquire(ctx context.Context) error {
	lk, id, err := rl.scan(ctx)
	if err != nil {
		return err
	}
	rl.mu.Lock()
	rl.lock = lk
	rl.lastOK = time.Now()
	rl.mu.Unlock()
	rl.nodeID.Store(id)
	rl.startRenew()
	return nil
}

// scan shuffles the candidate slots and grabs the first free one. Shuffling
// spreads contention so N pods don't all fight for slot 0.
func (rl *redisLease) scan(ctx context.Context) (*pulseredis.Lock, int64, error) {
	candidates := make([]int, rl.poolSize)
	for i := range candidates {
		candidates[i] = i
	}
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	for _, id := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		lk, err := rl.locker.Lock(ctx, strconv.Itoa(id))
		if err == nil {
			return lk, int64(id), nil
		}
		if errors.Is(err, pulseredis.ErrLockNotAcquired) {
			continue // held by another pod — try the next slot
		}
		return nil, 0, fmt.Errorf("snowflake: claim worker-id slot %d: %w", id, err)
	}
	return nil, 0, fmt.Errorf("%w: %d slots", ErrPoolExhausted, rl.poolSize)
}

func (rl *redisLease) startRenew() {
	rl.done = make(chan struct{})
	rl.wg.Add(1)
	lifecycle.SafeGo("snowflake-worker-id-renew", func() {
		defer rl.wg.Done()
		rl.renewLoop()
	}, func(name string, recovered any, stack []byte) {
		rl.log.Error("snowflake: worker-id renew goroutine panicked",
			zap.String("goroutine", name), zap.Any("recovered", recovered), zap.ByteString("stack", stack))
	})
}

func (rl *redisLease) renewLoop() {
	t := time.NewTicker(rl.renewInterval)
	defer t.Stop()
	for {
		select {
		case <-rl.done:
			return
		case <-t.C:
			rl.renewOnce()
		}
	}
}

func (rl *redisLease) renewOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), rl.renewInterval)
	defer cancel()
	ctx = tracing.WithGeneratedSpanContext(ctx)
	l := rl.log.ForContext(ctx)

	rl.mu.Lock()
	lk := rl.lock
	lastOK := rl.lastOK
	rl.mu.Unlock()

	if lk != nil {
		err := lk.Extend(ctx)
		if err == nil {
			rl.mu.Lock()
			rl.lastOK = time.Now()
			rl.mu.Unlock()
			return
		}
		if !errors.Is(err, pulseredis.ErrLockNotHeld) && time.Since(lastOK) < rl.ttl {
			// Transient failure (redis blip) but still within the TTL window: keep
			// the lease and retry next tick rather than fence on one bad renewal.
			l.Warn("snowflake: worker-id lease extend failed, will retry",
				zap.Int64("node", rl.nodeID.Load()), zap.Error(err))
			return
		}
		// Confirmed loss (ErrLockNotHeld) or we could not renew for longer than the
		// TTL — we can no longer prove we own the slot.
	}

	if !rl.fenced.Swap(true) {
		rl.metrics.setFenced(true)
		rl.metrics.incLeaseLost()
		l.Error("snowflake: worker-id lease lost; fencing generator and re-acquiring a slot",
			zap.Int64("node", rl.nodeID.Load()))
	}

	newLock, id, err := rl.scan(ctx)
	if err != nil {
		l.Error("snowflake: re-acquire worker-id slot failed; still fenced", zap.Error(err))
		return
	}
	rl.mu.Lock()
	rl.lock = newLock
	rl.lastOK = time.Now()
	rl.mu.Unlock()
	rl.nodeID.Store(id)
	rl.fenced.Store(false)
	rl.metrics.setFenced(false)
	rl.metrics.setWorkerID(id)
	l.Info("snowflake: re-acquired worker-id slot, resuming generation", zap.Int64("node", id))
}

// stop ends the renewal goroutine and releases the held slot.
func (rl *redisLease) stop(ctx context.Context) error {
	if rl.done != nil {
		close(rl.done)
		rl.wg.Wait()
	}
	rl.mu.Lock()
	lk := rl.lock
	rl.lock = nil
	rl.mu.Unlock()
	if lk != nil {
		if err := lk.Unlock(ctx); err != nil && !errors.Is(err, pulseredis.ErrLockNotHeld) {
			return fmt.Errorf("snowflake: release worker-id slot: %w", err)
		}
	}
	return nil
}
