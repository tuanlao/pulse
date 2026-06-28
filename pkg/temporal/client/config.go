// Package client is pulse's Temporal client component: a lifecycle.Component
// wrapping the Temporal SDK client used to start, signal, query and cancel
// workflows. A service that only orchestrates (starts) sagas needs only this; a
// service that executes them also needs pkg/temporal/worker. The facade
// pkg/temporal dials the connection once and the client component owns it
// (closing it on Stop).
package client

import "time"

// Config configures the Temporal client component. The connection (host,
// namespace, identity, TLS) lives in the shared ConnectionConfig passed via Deps;
// these are client-behavior defaults used by the StartWorkflow helper.
type Config struct {
	// DefaultTaskQueue is the task queue StartWorkflow uses when the caller's
	// StartWorkflowOptions leaves it empty. Default "".
	DefaultTaskQueue string `mapstructure:"default_task_queue"`
	// DefaultWorkflowRunTimeout bounds a single workflow run when the caller does
	// not set one. Default 0 (unbounded).
	DefaultWorkflowRunTimeout time.Duration `mapstructure:"default_workflow_run_timeout"`
	// DefaultWorkflowTaskTimeout bounds a single workflow task when the caller does
	// not set one. Default 10s.
	DefaultWorkflowTaskTimeout time.Duration `mapstructure:"default_workflow_task_timeout"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DefaultWorkflowTaskTimeout: 10 * time.Second,
	}
}

// ApplyDefaults fills empty fields from DefaultConfig. It is exported so the
// facade can normalize a composed config across packages.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.DefaultWorkflowTaskTimeout <= 0 {
		c.DefaultWorkflowTaskTimeout = d.DefaultWorkflowTaskTimeout
	}
}

// Option overrides Config fields programmatically.
type Option func(*Config)

// WithDefaultTaskQueue sets the default task queue for StartWorkflow.
func WithDefaultTaskQueue(q string) Option {
	return func(c *Config) { c.DefaultTaskQueue = q }
}

// WithDefaultWorkflowRunTimeout sets the default workflow run timeout.
func WithDefaultWorkflowRunTimeout(d time.Duration) Option {
	return func(c *Config) { c.DefaultWorkflowRunTimeout = d }
}
