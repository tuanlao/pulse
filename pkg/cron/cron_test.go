package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/trace"
)

func newSched(t *testing.T, deps Deps, opts ...Option) *Scheduler {
	t.Helper()
	s, err := New(DefaultConfig(), deps, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	return s
}

func waitSignal(t *testing.T, ch <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("timed out waiting for job to run")
	}
}

func TestJobRuns_AndTraceIDAlwaysPresent(t *testing.T) {
	ran := make(chan struct{}, 1)
	var validTrace atomic.Bool

	s := newSched(t, Deps{}) // no tracer -> tracing off, ids must still be generated
	_, err := s.AddJob(Every(20*time.Millisecond), "tick", func(ctx context.Context) error {
		if trace.SpanContextFromContext(ctx).IsValid() {
			validTrace.Store(true)
		}
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("AddJob: %v", err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waitSignal(t, ran, 2*time.Second)
	if !validTrace.Load() {
		t.Fatalf("expected a valid trace id in the job context even with tracing off")
	}
}

func TestPanicRecovered(t *testing.T) {
	ran := make(chan struct{}, 4)
	m, _ := NewMetrics(DefaultConfig().Metrics, nil)

	s := newSched(t, Deps{Metrics: m})
	_, _ = s.AddJob(Every(20*time.Millisecond), "boom", func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		panic("kaboom")
	})
	_ = s.Start(context.Background())

	// It must run at least twice — proving a panic in one run doesn't kill the
	// scheduler.
	waitSignal(t, ran, 2*time.Second)
	waitSignal(t, ran, 2*time.Second)

	if got := testutil.ToFloat64(m.runs.WithLabelValues("boom", "panic")); got < 1 {
		t.Fatalf("expected panic runs recorded, got %v", got)
	}
}

func TestJobTimeout(t *testing.T) {
	result := make(chan error, 1)

	s := newSched(t, Deps{}, WithJobTimeout(30*time.Millisecond))
	_, _ = s.AddJob(Every(20*time.Millisecond), "slow", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			select {
			case result <- ctx.Err():
			default:
			}
		case <-time.After(2 * time.Second):
			select {
			case result <- nil:
			default:
			}
		}
		return nil
	})
	_ = s.Start(context.Background())

	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected DeadlineExceeded, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("job did not observe timeout")
	}
}

func TestMetricsSuccessAndError(t *testing.T) {
	m, _ := NewMetrics(DefaultConfig().Metrics, nil)
	okRan := make(chan struct{}, 1)
	errRan := make(chan struct{}, 1)

	s := newSched(t, Deps{Metrics: m})
	_, _ = s.AddJob(Every(20*time.Millisecond), "ok", func(context.Context) error {
		select {
		case okRan <- struct{}{}:
		default:
		}
		return nil
	})
	_, _ = s.AddJob(Every(20*time.Millisecond), "bad", func(context.Context) error {
		select {
		case errRan <- struct{}{}:
		default:
		}
		return errors.New("nope")
	})
	_ = s.Start(context.Background())

	waitSignal(t, okRan, 2*time.Second)
	waitSignal(t, errRan, 2*time.Second)
	// Give the deferred metric recording a moment to run.
	time.Sleep(100 * time.Millisecond)

	if got := testutil.ToFloat64(m.runs.WithLabelValues("ok", "success")); got < 1 {
		t.Fatalf("expected success runs, got %v", got)
	}
	if got := testutil.ToFloat64(m.runs.WithLabelValues("bad", "error")); got < 1 {
		t.Fatalf("expected error runs, got %v", got)
	}
}

func TestSingletonPreventsOverlap(t *testing.T) {
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	ran := make(chan struct{}, 1)

	s := newSched(t, Deps{}) // Singleton true by default
	_, _ = s.AddJob(Every(20*time.Millisecond), "long", func(context.Context) error {
		n := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if n <= old || maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case ran <- struct{}{}:
		default:
		}
		time.Sleep(120 * time.Millisecond) // longer than the interval
		concurrent.Add(-1)
		return nil
	})
	_ = s.Start(context.Background())

	waitSignal(t, ran, 2*time.Second)
	time.Sleep(500 * time.Millisecond) // let several intervals elapse

	if got := maxConcurrent.Load(); got > 1 {
		t.Fatalf("singleton mode failed: max concurrency = %d", got)
	}
}

func TestStartStopGraceful(t *testing.T) {
	s := newSched(t, Deps{})
	_, _ = s.AddJob(Every(time.Hour), "noop", func(context.Context) error { return nil })
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if s.Name() != "cron" {
		t.Fatalf("Name = %q", s.Name())
	}
}

// fakeLocker implements gocron.Locker so the distributed-lock path can be wired
// without a real redis.
type fakeLocker struct{ locks atomic.Int32 }

func (f *fakeLocker) Lock(_ context.Context, _ string) (gocron.Lock, error) {
	f.locks.Add(1)
	return &fakeLock{}, nil
}

type fakeLock struct{}

func (f *fakeLock) Unlock(context.Context) error { return nil }

func TestDistributedLockerWired(t *testing.T) {
	fl := &fakeLocker{}
	ran := make(chan struct{}, 1)

	s := newSched(t, Deps{Locker: fl})
	_, _ = s.AddJob(Every(20*time.Millisecond), "locked", func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	})
	_ = s.Start(context.Background())

	waitSignal(t, ran, 2*time.Second)
	if fl.locks.Load() < 1 {
		t.Fatalf("expected the distributed locker to be invoked")
	}
}

func TestDefaultsAndOptions(t *testing.T) {
	c := DefaultConfig()
	if !c.Enabled || !c.Singleton || c.Lock.Enabled {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	s, err := New(Config{Enabled: true}, Deps{}, WithTimezone("Asia/Ho_Chi_Minh"), WithJobTimeout(time.Second))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	if s.Config().Timezone != "Asia/Ho_Chi_Minh" || s.Config().JobTimeout != time.Second {
		t.Fatalf("options not applied: %+v", s.Config())
	}
}

func TestJobsFromConfig(t *testing.T) {
	ran := make(chan struct{}, 1)
	cfg := DefaultConfig()
	cfg.Jobs = map[string]JobConfig{
		"configured": {Enabled: true, Every: 20 * time.Millisecond},
		"disabled":   {Enabled: false, Every: 20 * time.Millisecond},
	}
	s, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	s.Register("configured", func(context.Context) error {
		select {
		case ran <- struct{}{}:
		default:
		}
		return nil
	})
	// "disabled" intentionally has no handler — must not fail because it's disabled.

	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitSignal(t, ran, 2*time.Second)
}

func TestJobsFromConfig_MissingHandlerFailsFast(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Jobs = map[string]JobConfig{
		"orphan": {Enabled: true, Every: time.Second},
	}
	s, err := New(cfg, Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop(context.Background()) })

	// No Register("orphan", ...) -> Start must fail fast.
	if err := s.Start(context.Background()); err == nil {
		t.Fatalf("expected Start to fail for a config job with no handler")
	}
}

func TestJobsFromConfig_BadSchedule(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Jobs = map[string]JobConfig{
		"noschedule": {Enabled: true}, // neither cron nor every
	}
	s, _ := New(cfg, Deps{})
	t.Cleanup(func() { _ = s.Stop(context.Background()) })
	s.Register("noschedule", func(context.Context) error { return nil })
	if err := s.Start(context.Background()); err == nil {
		t.Fatalf("expected error for job with no schedule")
	}
}
