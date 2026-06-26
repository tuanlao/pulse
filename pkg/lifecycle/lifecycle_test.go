package lifecycle

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeComponent records start/stop order into a shared log.
type fakeComponent struct {
	name         string
	rec          *[]string
	mu           *sync.Mutex
	startErr     error
	stopErr      error
	stopDelay    time.Duration
	panicOnStart bool
}

func (f *fakeComponent) Name() string { return f.name }

func (f *fakeComponent) Start(context.Context) error {
	if f.panicOnStart {
		panic("boom")
	}
	if f.startErr != nil {
		return f.startErr
	}
	f.mu.Lock()
	*f.rec = append(*f.rec, "start:"+f.name)
	f.mu.Unlock()
	return nil
}

func (f *fakeComponent) Stop(ctx context.Context) error {
	if f.stopDelay > 0 {
		select {
		case <-time.After(f.stopDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	*f.rec = append(*f.rec, "stop:"+f.name)
	f.mu.Unlock()
	return f.stopErr
}

// instantSignal returns a notify func whose context is already cancelled, so
// Run proceeds to shutdown immediately.
func instantSignal() func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
	return func(ctx context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		c, cancel := context.WithCancel(ctx)
		cancel()
		return c, func() {}
	}
}

func newManager(t *testing.T) (*Manager, *[]string, *sync.Mutex) {
	t.Helper()
	var rec []string
	var mu sync.Mutex
	m := New(DefaultConfig(), nil)
	m.notify = instantSignal()
	return m, &rec, &mu
}

func TestManager_StartThenReverseShutdown(t *testing.T) {
	m, rec, mu := newManager(t)
	m.Register(
		&fakeComponent{name: "tracing", rec: rec, mu: mu},
		&fakeComponent{name: "metrics", rec: rec, mu: mu},
		&fakeComponent{name: "http", rec: rec, mu: mu},
	)

	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []string{
		"start:tracing", "start:metrics", "start:http",
		"stop:http", "stop:metrics", "stop:tracing",
	}
	if len(*rec) != len(want) {
		t.Fatalf("order = %v, want %v", *rec, want)
	}
	for i := range want {
		if (*rec)[i] != want[i] {
			t.Fatalf("order = %v, want %v", *rec, want)
		}
	}
}

func TestManager_StartFailureRollsBack(t *testing.T) {
	m, rec, mu := newManager(t)
	m.Register(
		&fakeComponent{name: "a", rec: rec, mu: mu},
		&fakeComponent{name: "b", rec: rec, mu: mu, startErr: errors.New("nope")},
		&fakeComponent{name: "c", rec: rec, mu: mu},
	)

	err := m.Run(context.Background())
	if err == nil {
		t.Fatalf("expected start error")
	}
	// "a" started, "b" failed (and is not added to started), "c" never started.
	// Rollback stops only "a".
	want := []string{"start:a", "stop:a"}
	if len(*rec) != len(want) || (*rec)[0] != want[0] || (*rec)[1] != want[1] {
		t.Fatalf("order = %v, want %v", *rec, want)
	}
}

func TestManager_StopErrorsAggregated(t *testing.T) {
	m, rec, mu := newManager(t)
	e1 := errors.New("stop-a-fail")
	e2 := errors.New("stop-b-fail")
	m.Register(
		&fakeComponent{name: "a", rec: rec, mu: mu, stopErr: e1},
		&fakeComponent{name: "b", rec: rec, mu: mu, stopErr: e2},
	)

	err := m.Run(context.Background())
	if err == nil {
		t.Fatalf("expected aggregated stop error")
	}
	if !errors.Is(err, e1) || !errors.Is(err, e2) {
		t.Fatalf("expected both errors joined, got %v", err)
	}
}

func TestManager_PanicInStartBecomesError(t *testing.T) {
	m, rec, mu := newManager(t)
	m.Register(&fakeComponent{name: "boom", rec: rec, mu: mu, panicOnStart: true})

	err := m.Run(context.Background())
	if err == nil {
		t.Fatalf("expected error from panic")
	}
}

func TestManager_ShutdownTimeoutHonored(t *testing.T) {
	var rec []string
	var mu sync.Mutex
	m := New(DefaultConfig(), nil, WithShutdownTimeout(20*time.Millisecond))
	m.notify = instantSignal()
	m.Register(&fakeComponent{name: "slow", rec: &rec, mu: &mu, stopDelay: time.Second})

	start := time.Now()
	err := m.Run(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown did not honor timeout, took %v", elapsed)
	}
}

func TestSafeGo_RecoversPanic(t *testing.T) {
	done := make(chan struct{})
	var got any
	SafeGo("worker", func() { panic("kaboom") }, func(_ string, r any, _ []byte) {
		got = r
		close(done)
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("onPanic not called")
	}
	if got != "kaboom" {
		t.Fatalf("recovered = %v, want kaboom", got)
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.ShutdownTimeout != 30*time.Second || c.StartTimeout != 15*time.Second {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if len(c.Signals) != 2 {
		t.Fatalf("expected 2 default signals, got %v", c.Signals)
	}
}
