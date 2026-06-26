package server

import (
	"context"
	"sync"
	"time"
)

// ReadinessCheck reports whether a dependency is ready to serve traffic. It
// returns nil when ready. Components (DB, redis, kafka, ...) register their own
// checks without httpx ever importing them — httpx only knows this func type.
type ReadinessCheck func(ctx context.Context) error

// ReadinessRegistry holds named readiness checks evaluated by /readyz.
type ReadinessRegistry struct {
	mu              sync.RWMutex
	checks          map[string]ReadinessCheck
	perCheckTimeout time.Duration
}

// NewReadinessRegistry creates a registry. A non-positive timeout defaults to 2s.
func NewReadinessRegistry(perCheckTimeout time.Duration) *ReadinessRegistry {
	if perCheckTimeout <= 0 {
		perCheckTimeout = 2 * time.Second
	}
	return &ReadinessRegistry{
		checks:          make(map[string]ReadinessCheck),
		perCheckTimeout: perCheckTimeout,
	}
}

// Register adds or replaces a named check. A nil check is ignored.
func (r *ReadinessRegistry) Register(name string, check ReadinessCheck) {
	if check == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = check
}

// Result is the outcome of one readiness check.
type Result struct {
	Status string `json:"status"`          // "ok" or "fail"
	Error  string `json:"error,omitempty"` // populated when Status == "fail"
}

// Evaluate runs all checks concurrently, each bounded by perCheckTimeout, and
// returns a per-name result map and whether every check passed. With no checks
// registered the service is considered ready.
func (r *ReadinessRegistry) Evaluate(ctx context.Context) (map[string]Result, bool) {
	r.mu.RLock()
	checks := make(map[string]ReadinessCheck, len(r.checks))
	for k, v := range r.checks {
		checks[k] = v
	}
	timeout := r.perCheckTimeout
	r.mu.RUnlock()

	results := make(map[string]Result, len(checks))
	if len(checks) == 0 {
		return results, true
	}

	type named struct {
		name string
		err  error
	}
	ch := make(chan named, len(checks))
	for name, check := range checks {
		go func(name string, check ReadinessCheck) {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			// Race the check against its own timeout so a check that ignores cctx
			// cannot wedge the receive loop below (and hang /readyz). The inner
			// goroutine may linger until the check finally returns, but the result
			// is reported within timeout either way.
			done := make(chan error, 1)
			go func() { done <- check(cctx) }()
			select {
			case err := <-done:
				ch <- named{name: name, err: err}
			case <-cctx.Done():
				ch <- named{name: name, err: cctx.Err()}
			}
		}(name, check)
	}

	allReady := true
	for i := 0; i < len(checks); i++ {
		res := <-ch
		if res.err != nil {
			allReady = false
			results[res.name] = Result{Status: "fail", Error: res.err.Error()}
		} else {
			results[res.name] = Result{Status: "ok"}
		}
	}
	return results, allReady
}
