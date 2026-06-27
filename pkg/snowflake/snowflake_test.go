package snowflake

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if !c.Enabled || c.Epoch != defaultEpoch || c.NodeBits != 10 || c.StepBits != 12 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if c.WorkerID.Strategy != StrategyStatic || c.WorkerID.StatefulSet.PodNameEnv != "POD_NAME" {
		t.Fatalf("unexpected worker-id defaults: %+v", c.WorkerID)
	}
}

func TestApplyDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	if c.Epoch != defaultEpoch || c.NodeBits != 10 || c.StepBits != 12 || c.MaxClockDriftWait != 5*time.Millisecond {
		t.Fatalf("zero config not defaulted: %+v", c)
	}
	if c.WorkerID.Strategy != StrategyStatic {
		t.Fatalf("strategy not defaulted: %q", c.WorkerID.Strategy)
	}
	if c.WorkerID.Redis.TTL != 15*time.Second {
		t.Fatalf("ttl not defaulted: %v", c.WorkerID.Redis.TTL)
	}
	// RenewInterval derives to TTL/3.
	if c.WorkerID.Redis.RenewInterval != 5*time.Second {
		t.Fatalf("renew interval = %v, want TTL/3 (5s)", c.WorkerID.Redis.RenewInterval)
	}
}

func TestOptions(t *testing.T) {
	c := DefaultConfig()
	for _, opt := range []Option{
		WithStaticWorkerID(9),
		WithNodeBits(8),
		WithStepBits(14),
		WithEpoch(1000),
		WithRedisTTL(30 * time.Second),
		WithPodNameEnv("MY_POD"),
		WithKeyPrefix("svc:sf"),
	} {
		opt(&c)
	}
	if c.WorkerID.Strategy != StrategyStatic || c.WorkerID.Static != 9 {
		t.Fatalf("static option not applied: %+v", c.WorkerID)
	}
	if c.NodeBits != 8 || c.StepBits != 14 || c.Epoch != 1000 {
		t.Fatalf("layout options not applied: %+v", c)
	}
	if c.WorkerID.Redis.TTL != 30*time.Second || c.WorkerID.StatefulSet.PodNameEnv != "MY_POD" || c.WorkerID.Redis.KeyPrefix != "svc:sf" {
		t.Fatalf("redis/statefulset options not applied: %+v", c.WorkerID)
	}

	c2 := DefaultConfig()
	WithRedisStrategy("redis:6379")(&c2)
	if c2.WorkerID.Strategy != StrategyRedis || c2.WorkerID.Redis.Redis.Address != "redis:6379" {
		t.Fatalf("redis strategy option not applied: %+v", c2.WorkerID)
	}
}

func TestNew_InvalidLayout(t *testing.T) {
	if _, err := New(DefaultConfig(), Deps{}, WithNodeBits(20)); err == nil {
		// 20 + 12 (default step) = 32 > 22
		t.Fatalf("expected error for node_bits+step_bits > 22")
	}
	if _, err := New(DefaultConfig(), Deps{}, WithStaticWorkerID(0), WithNodeBits(10), WithStepBits(12)); err != nil {
		t.Fatalf("valid 10+12 layout rejected: %v", err)
	}
}

func TestNew_UnknownStrategy(t *testing.T) {
	c := DefaultConfig()
	c.WorkerID.Strategy = "bogus"
	if _, err := New(c, Deps{}); err == nil {
		t.Fatalf("expected error for unknown strategy")
	}
}

func TestStaticOutOfRange(t *testing.T) {
	// static id 2000 does not fit in 10 node bits (max 1023).
	if _, err := New(DefaultConfig(), Deps{}, WithStaticWorkerID(2000)); !errors.Is(err, ErrWorkerIDOutOfRange) {
		t.Fatalf("expected ErrWorkerIDOutOfRange, got %v", err)
	}
}

func newStatic(t *testing.T, id int64, opts ...Option) *Generator {
	t.Helper()
	g, err := New(DefaultConfig(), Deps{}, append([]Option{WithStaticWorkerID(id)}, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return g
}

func TestGenerate_SequenceWithinMillis(t *testing.T) {
	g := newStatic(t, 2, WithNodeBits(10), WithStepBits(4)) // maxStep = 15
	fixed := time.UnixMilli(1_700_000_000_000)
	g.nowFunc = func() time.Time { return fixed }

	n := int(g.MaxStep()) + 1
	seen := make(map[ID]struct{}, n)
	var last ID
	for i := 0; i < n; i++ {
		id := g.Generate()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id at step %d", i)
		}
		seen[id] = struct{}{}
		if i > 0 && id <= last {
			t.Fatalf("ids not strictly increasing at step %d", i)
		}
		last = id
		if g.Step(id) != int64(i) {
			t.Fatalf("step(id) = %d, want %d", g.Step(id), i)
		}
		if g.Node(id) != 2 {
			t.Fatalf("node(id) = %d, want 2", g.Node(id))
		}
	}
}

func TestGenerate_SequenceOverflowAdvancesMillis(t *testing.T) {
	// StepBits must be > 0 (0 is treated as "unset" by applyDefaults). Use 1 ->
	// maxStep 1 -> 2 ids/ms, so the 3rd same-millisecond id overflows the sequence.
	g := newStatic(t, 1, WithNodeBits(10), WithStepBits(1))
	base := int64(1_700_000_000_000)
	var calls atomic.Int64
	g.nowFunc = func() time.Time {
		if calls.Add(1) <= 3 {
			return time.UnixMilli(base)
		}
		return time.UnixMilli(base + 1)
	}

	id1 := g.Generate() // call 1 -> base, step 0
	id2 := g.Generate() // call 2 -> base, step 1
	id3 := g.Generate() // call 3 -> base, overflow -> waitNextMillis call 4 -> base+1
	if g.Time(id1) != base || g.Time(id2) != base {
		t.Fatalf("id1/id2 should be at base: %d, %d", g.Time(id1), g.Time(id2))
	}
	if g.Time(id3) != base+1 {
		t.Fatalf("id3 time = %d, want %d (overflow should advance the millisecond)", g.Time(id3), base+1)
	}
	if !(id1 < id2 && id2 < id3) {
		t.Fatalf("ids not strictly increasing: %d %d %d", id1, id2, id3)
	}
}

func TestGenerate_ClockBackwardsFails(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClockDriftWait = 2 * time.Millisecond
	g, err := New(cfg, Deps{}, WithStaticWorkerID(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base := int64(1_700_000_000_000)
	var calls atomic.Int64
	g.nowFunc = func() time.Time {
		if calls.Add(1) == 1 {
			return time.UnixMilli(base)
		}
		return time.UnixMilli(base - 5) // stuck behind
	}

	_ = g.Generate()
	if _, err := g.TryGenerate(); !errors.Is(err, ErrClockBackwards) {
		t.Fatalf("expected ErrClockBackwards, got %v", err)
	}
}

func TestGenerate_ClockBackwardsRecovers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClockDriftWait = 200 * time.Millisecond
	g, err := New(cfg, Deps{}, WithStaticWorkerID(1))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	base := int64(1_700_000_000_000)
	var calls atomic.Int64
	g.nowFunc = func() time.Time {
		switch calls.Add(1) {
		case 1:
			return time.UnixMilli(base) // id1
		case 2:
			return time.UnixMilli(base - 1) // small backward drift
		default:
			return time.UnixMilli(base) // recovered
		}
	}

	_ = g.Generate()
	id2, err := g.TryGenerate()
	if err != nil {
		t.Fatalf("expected recovery, got %v", err)
	}
	if g.Time(id2) != base {
		t.Fatalf("id2 time = %d, want %d", g.Time(id2), base)
	}
}

func TestPackUnpack(t *testing.T) {
	g := newStatic(t, 0)
	cases := []struct{ tms, node, step int64 }{
		{g.epoch, 0, 0},
		{g.epoch + 1, 1, 1},
		{g.epoch + 123456, 1023, 4095},
		{g.epoch + 999_999_999, 512, 2048},
	}
	for _, c := range cases {
		id := ID((c.tms-g.epoch)<<g.timeShift | c.node<<g.nodeShift | c.step)
		if id < 0 {
			t.Fatalf("sign bit set for %+v", c)
		}
		if g.Time(id) != c.tms {
			t.Fatalf("Time = %d, want %d", g.Time(id), c.tms)
		}
		if g.Node(id) != c.node {
			t.Fatalf("Node = %d, want %d", g.Node(id), c.node)
		}
		if g.Step(id) != c.step {
			t.Fatalf("Step = %d, want %d", g.Step(id), c.step)
		}
	}
}

func TestGenerate_ConcurrentUnique(t *testing.T) {
	g := newStatic(t, 1)
	const goroutines, per = 16, 2000
	results := make([][]ID, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids := make([]ID, per)
			for j := range ids {
				ids[j] = g.Generate()
			}
			results[i] = ids
		}(i)
	}
	wg.Wait()

	seen := make(map[ID]struct{}, goroutines*per)
	for _, ids := range results {
		for _, id := range ids {
			if _, dup := seen[id]; dup {
				t.Fatalf("duplicate id %d across goroutines", id)
			}
			seen[id] = struct{}{}
		}
	}
	if len(seen) != goroutines*per {
		t.Fatalf("got %d unique ids, want %d", len(seen), goroutines*per)
	}
}

func TestDisabledGenerator(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false
	g, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New disabled: %v", err)
	}
	if _, err := g.TryGenerate(); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected ErrDisabled, got %v", err)
	}
	// Lifecycle methods are no-ops on a disabled generator.
	if err := g.Start(context.Background()); err != nil {
		t.Fatalf("disabled Start: %v", err)
	}
	if err := g.Stop(context.Background()); err != nil {
		t.Fatalf("disabled Stop: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatalf("expected Generate to panic on a disabled generator")
		}
	}()
	_ = g.Generate()
}

func TestStatefulSetGeneratorWiring(t *testing.T) {
	t.Setenv("SNOWFLAKE_POD_NAME", "svc-4")
	g, err := New(DefaultConfig(), Deps{}, WithStatefulSetStrategy(), WithPodNameEnv("SNOWFLAKE_POD_NAME"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !g.Ready() || g.WorkerID() != 4 {
		t.Fatalf("statefulset generator not ready or wrong node: ready=%v node=%d", g.Ready(), g.WorkerID())
	}
	id := g.Generate()
	if g.Node(id) != 4 {
		t.Fatalf("node(id) = %d, want 4", g.Node(id))
	}
}
