package cron

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CronMetrics holds per-job Prometheus collectors. Following pulse's registry
// discipline it registers into a provided registry (so it can share the
// server's /metrics endpoint) or a fresh package-owned one — never the global
// default registry.
type CronMetrics struct {
	reg      *prometheus.Registry
	runs     *prometheus.CounterVec // labels: job, status
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
}

// NewMetrics builds the collectors and registers them into reg (or a fresh
// registry when reg is nil).
func NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*CronMetrics, error) {
	// Disabled → return a nil collector set. Callers wire the result into
	// Deps.Metrics (a concrete *CronMetrics), and the scheduler skips every metric
	// call when Deps.Metrics is nil, so cron.metrics.enabled=false truly disables
	// per-job metrics instead of being a no-op flag.
	if !cfg.Enabled {
		return nil, nil
	}
	d := DefaultConfig().Metrics
	if cfg.Namespace == "" {
		cfg.Namespace = d.Namespace
	}
	if cfg.Subsystem == "" {
		cfg.Subsystem = d.Subsystem
	}
	if len(cfg.Buckets) == 0 {
		cfg.Buckets = d.Buckets
	}
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &CronMetrics{
		reg: reg,
		runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "runs_total",
			Help:      "Total cron job runs, by job and status (success/error/panic).",
		}, []string{"job", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "run_duration_seconds",
			Help:      "Cron job run duration in seconds, by job.",
			Buckets:   cfg.Buckets,
		}, []string{"job"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "jobs_in_flight",
			Help:      "Cron jobs currently executing.",
		}),
	}
	for _, c := range []prometheus.Collector{m.runs, m.duration, m.inFlight} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in.
func (m *CronMetrics) Registry() *prometheus.Registry { return m.reg }

func (m *CronMetrics) start() { m.inFlight.Inc() }

func (m *CronMetrics) finish(job, status string, d time.Duration) {
	m.inFlight.Dec()
	m.runs.WithLabelValues(job, status).Inc()
	m.duration.WithLabelValues(job).Observe(d.Seconds())
}
