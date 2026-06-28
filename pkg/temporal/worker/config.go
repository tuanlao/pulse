// Package worker is pulse's Temporal worker component: a lifecycle.Component that
// polls a task queue and executes workflows and activities. It is built from a
// shared Temporal client (passed via Deps, never owned). This is where the
// memory/OOM controls live: the sticky workflow cache size, static concurrency
// caps, and the opt-in resource-based tuner. (The other half of history control —
// Continue-As-New — lives in pkg/temporal/saga, since only a workflow can call it.)
package worker

import "time"

// Config configures the worker and, crucially, bounds its memory footprint.
type Config struct {
	// Enabled toggles the worker independently of the rest of temporal: a
	// frontend service that only starts workflows leaves it false. Default true.
	Enabled bool `mapstructure:"enabled"`
	// TaskQueue is the queue this worker polls. REQUIRED when enabled.
	TaskQueue string `mapstructure:"task_queue"`
	// StopTimeout bounds graceful shutdown (in-flight task draining). Default 30s.
	StopTimeout time.Duration `mapstructure:"stop_timeout"`
	// DeadlockDetectionTimeout bounds a single workflow task before it is treated
	// as deadlocked. Default 1s (the SDK default).
	DeadlockDetectionTimeout time.Duration `mapstructure:"deadlock_detection_timeout"`
	// EnableSessionWorker enables activity sessions. Mutually exclusive with the
	// resource tuner. Default false.
	EnableSessionWorker bool `mapstructure:"enable_session_worker"`

	// Sticky bounds the in-memory workflow cache (the primary OOM lever).
	Sticky StickyConfig `mapstructure:"sticky"`
	// Concurrency caps in-flight tasks. Used only when ResourceTuner is off.
	Concurrency ConcurrencyConfig `mapstructure:"concurrency"`
	// ResourceTuner dynamically throttles slots to keep memory/CPU under a target.
	// Opt-in; mutually exclusive with the static Concurrency execution caps.
	ResourceTuner ResourceTunerConfig `mapstructure:"resource_tuner"`
}

// StickyConfig bounds the sticky workflow cache.
type StickyConfig struct {
	// CacheSize is how many workflow histories stay cached in memory so workers
	// avoid replaying from the beginning. This is the primary lever against worker
	// OOM. IMPORTANT: it is PROCESS-GLOBAL in the Temporal SDK — shared by every
	// worker in the process; the first non-zero value wins. Default 10000.
	CacheSize int `mapstructure:"cache_size"`
	// ScheduleToStartTimeout is the sticky task schedule-to-start timeout. Default
	// 5s.
	ScheduleToStartTimeout time.Duration `mapstructure:"schedule_to_start_timeout"`
}

// ConcurrencyConfig caps the number of concurrently executing/polling tasks. The
// execution-size caps bound how much workflow/activity state is held in memory at
// once.
type ConcurrencyConfig struct {
	MaxConcurrentWorkflowTaskExecutionSize  int `mapstructure:"max_concurrent_workflow_task_execution_size"`
	MaxConcurrentActivityExecutionSize      int `mapstructure:"max_concurrent_activity_execution_size"`
	MaxConcurrentLocalActivityExecutionSize int `mapstructure:"max_concurrent_local_activity_execution_size"`
	MaxConcurrentWorkflowTaskPollers        int `mapstructure:"max_concurrent_workflow_task_pollers"`
	MaxConcurrentActivityTaskPollers        int `mapstructure:"max_concurrent_activity_task_pollers"`
}

// ResourceTunerConfig configures the resource-based tuner. When enabled the
// worker auto-adjusts slot counts to keep system memory/CPU under the targets —
// the strongest guard against OOM. It is MUTUALLY EXCLUSIVE with the static
// Concurrency execution caps (the caps are ignored when this is on).
type ResourceTunerConfig struct {
	// Enabled toggles the resource-based tuner. Default false (static caps apply).
	Enabled bool `mapstructure:"enabled"`
	// TargetMemory is the target system memory usage (0,1]. Must be > 0 when
	// enabled. Default 0.8.
	TargetMemory float64 `mapstructure:"target_memory"`
	// TargetCPU is the target system CPU usage (0,1]. Must be > 0 when enabled.
	// Default 0.9.
	TargetCPU float64 `mapstructure:"target_cpu"`
	// ActivityRampThrottle paces how fast activity slots ramp up. Default 50ms.
	ActivityRampThrottle time.Duration `mapstructure:"activity_ramp_throttle"`
	// WorkflowRampThrottle paces how fast workflow slots ramp up. Default 0.
	WorkflowRampThrottle time.Duration `mapstructure:"workflow_ramp_throttle"`
}

// DefaultConfig returns Config with sensible, memory-safe defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:                  true,
		StopTimeout:              30 * time.Second,
		DeadlockDetectionTimeout: time.Second,
		Sticky: StickyConfig{
			CacheSize:              10000,
			ScheduleToStartTimeout: 5 * time.Second,
		},
		Concurrency: ConcurrencyConfig{
			MaxConcurrentWorkflowTaskExecutionSize:  1000,
			MaxConcurrentActivityExecutionSize:      1000,
			MaxConcurrentLocalActivityExecutionSize: 1000,
			MaxConcurrentWorkflowTaskPollers:        2,
			MaxConcurrentActivityTaskPollers:        2,
		},
		ResourceTuner: ResourceTunerConfig{
			Enabled:              false,
			TargetMemory:         0.8,
			TargetCPU:            0.9,
			ActivityRampThrottle: 50 * time.Millisecond,
		},
	}
}

// ApplyDefaults fills empty fields from DefaultConfig. Exported so the facade can
// normalize a composed config across packages.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.StopTimeout <= 0 {
		c.StopTimeout = d.StopTimeout
	}
	if c.DeadlockDetectionTimeout <= 0 {
		c.DeadlockDetectionTimeout = d.DeadlockDetectionTimeout
	}
	if c.Sticky.CacheSize <= 0 {
		c.Sticky.CacheSize = d.Sticky.CacheSize
	}
	if c.Sticky.ScheduleToStartTimeout <= 0 {
		c.Sticky.ScheduleToStartTimeout = d.Sticky.ScheduleToStartTimeout
	}
	cc, dc := &c.Concurrency, d.Concurrency
	if cc.MaxConcurrentWorkflowTaskExecutionSize <= 0 {
		cc.MaxConcurrentWorkflowTaskExecutionSize = dc.MaxConcurrentWorkflowTaskExecutionSize
	}
	if cc.MaxConcurrentActivityExecutionSize <= 0 {
		cc.MaxConcurrentActivityExecutionSize = dc.MaxConcurrentActivityExecutionSize
	}
	if cc.MaxConcurrentLocalActivityExecutionSize <= 0 {
		cc.MaxConcurrentLocalActivityExecutionSize = dc.MaxConcurrentLocalActivityExecutionSize
	}
	if cc.MaxConcurrentWorkflowTaskPollers <= 0 {
		cc.MaxConcurrentWorkflowTaskPollers = dc.MaxConcurrentWorkflowTaskPollers
	}
	if cc.MaxConcurrentActivityTaskPollers <= 0 {
		cc.MaxConcurrentActivityTaskPollers = dc.MaxConcurrentActivityTaskPollers
	}
	if c.ResourceTuner.Enabled {
		if c.ResourceTuner.TargetMemory <= 0 {
			c.ResourceTuner.TargetMemory = d.ResourceTuner.TargetMemory
		}
		if c.ResourceTuner.TargetCPU <= 0 {
			c.ResourceTuner.TargetCPU = d.ResourceTuner.TargetCPU
		}
		if c.ResourceTuner.ActivityRampThrottle <= 0 {
			c.ResourceTuner.ActivityRampThrottle = d.ResourceTuner.ActivityRampThrottle
		}
	}
}

// Option overrides Config fields programmatically.
type Option func(*Config)

// WithTaskQueue sets the task queue the worker polls.
func WithTaskQueue(q string) Option { return func(c *Config) { c.TaskQueue = q } }

// WithStickyCacheSize sets the process-global sticky workflow cache size.
func WithStickyCacheSize(n int) Option { return func(c *Config) { c.Sticky.CacheSize = n } }

// WithResourceTuner enables the resource-based tuner with the given memory/CPU
// targets (each in (0,1]).
func WithResourceTuner(targetMemory, targetCPU float64) Option {
	return func(c *Config) {
		c.ResourceTuner.Enabled = true
		c.ResourceTuner.TargetMemory = targetMemory
		c.ResourceTuner.TargetCPU = targetCPU
	}
}

// WithMaxConcurrentActivities sets the static activity execution cap.
func WithMaxConcurrentActivities(n int) Option {
	return func(c *Config) { c.Concurrency.MaxConcurrentActivityExecutionSize = n }
}
