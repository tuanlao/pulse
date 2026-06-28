package worker

import "testing"

func TestApplyDefaults(t *testing.T) {
	var c Config
	c.ApplyDefaults()
	d := DefaultConfig()
	if c.StopTimeout != d.StopTimeout {
		t.Errorf("StopTimeout = %v, want %v", c.StopTimeout, d.StopTimeout)
	}
	if c.Sticky.CacheSize != d.Sticky.CacheSize {
		t.Errorf("Sticky.CacheSize = %d, want %d", c.Sticky.CacheSize, d.Sticky.CacheSize)
	}
	if c.Concurrency.MaxConcurrentActivityExecutionSize != d.Concurrency.MaxConcurrentActivityExecutionSize {
		t.Errorf("MaxConcurrentActivityExecutionSize = %d, want %d",
			c.Concurrency.MaxConcurrentActivityExecutionSize, d.Concurrency.MaxConcurrentActivityExecutionSize)
	}
}

func TestBuildWorkerOptionsStaticCaps(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TaskQueue = "tq"
	opts, err := buildWorkerOptions(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Tuner != nil {
		t.Fatal("expected no tuner when resource_tuner is disabled")
	}
	if opts.MaxConcurrentActivityExecutionSize != 1000 {
		t.Errorf("MaxConcurrentActivityExecutionSize = %d, want 1000", opts.MaxConcurrentActivityExecutionSize)
	}
	if opts.MaxConcurrentWorkflowTaskExecutionSize != 1000 {
		t.Errorf("MaxConcurrentWorkflowTaskExecutionSize = %d, want 1000", opts.MaxConcurrentWorkflowTaskExecutionSize)
	}
	if opts.MaxConcurrentActivityTaskPollers != 2 {
		t.Errorf("MaxConcurrentActivityTaskPollers = %d, want 2", opts.MaxConcurrentActivityTaskPollers)
	}
}

func TestBuildWorkerOptionsResourceTuner(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TaskQueue = "tq"
	cfg.ResourceTuner.Enabled = true
	opts, err := buildWorkerOptions(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Tuner == nil {
		t.Fatal("expected a tuner when resource_tuner is enabled")
	}
	// The static execution caps must NOT be set alongside the tuner (mutually
	// exclusive in the SDK).
	if opts.MaxConcurrentActivityExecutionSize != 0 {
		t.Errorf("MaxConcurrentActivityExecutionSize = %d, want 0 (tuner active)", opts.MaxConcurrentActivityExecutionSize)
	}
	if opts.MaxConcurrentWorkflowTaskExecutionSize != 0 {
		t.Errorf("MaxConcurrentWorkflowTaskExecutionSize = %d, want 0 (tuner active)", opts.MaxConcurrentWorkflowTaskExecutionSize)
	}
	// Pollers are not mutually exclusive with the tuner and stay set.
	if opts.MaxConcurrentActivityTaskPollers != 2 {
		t.Errorf("MaxConcurrentActivityTaskPollers = %d, want 2", opts.MaxConcurrentActivityTaskPollers)
	}
}

func TestBuildWorkerOptionsResourceTunerInvalid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TaskQueue = "tq"
	cfg.ResourceTuner.Enabled = true
	cfg.ResourceTuner.TargetMemory = 0
	cfg.ResourceTuner.TargetCPU = 0
	if _, err := buildWorkerOptions(cfg); err == nil {
		t.Fatal("expected an error when tuner targets are zero")
	}
}

func TestNewRequiresClientAndTaskQueue(t *testing.T) {
	if _, err := New(DefaultConfig(), Deps{}); err == nil {
		t.Fatal("expected an error when Deps.Client is nil")
	}
}
