package redis

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds per-command Prometheus collectors. Following pulse's registry
// discipline it registers into a provided registry (typically the server's, so
// /metrics exposes redis metrics alongside the rest) — never the global default
// registry and never the OTel->Prometheus bridge.
//
// Label cardinality is bounded by using the command VERB only (GET/SET/...),
// never the key; pipelines collapse to a single "PIPELINE" series.
type Metrics struct {
	reg      *prometheus.Registry
	duration *prometheus.HistogramVec // labels: command, status
	total    *prometheus.CounterVec   // labels: command, status
	cache    *prometheus.CounterVec   // labels: command, result (hit|miss)
}

// NewMetrics builds the collectors and registers them into reg (or a fresh
// registry when reg is nil). It honors cfg.Enabled: when disabled it returns a
// nil *Metrics so wiring the result into Deps.Metrics truly disables per-command
// metrics rather than being a no-op flag.
func NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*Metrics, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg.applyDefaults(DefaultConfig().Metrics)
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &Metrics{
		reg: reg,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "command_duration_seconds",
			Help:      "Redis command duration in seconds, by command verb and status (ok/error).",
			Buckets:   cfg.Buckets,
		}, []string{"command", "status"}),
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "commands_total",
			Help:      "Total redis commands, by command verb and status (ok/error).",
		}, []string{"command", "status"}),
		cache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "cache_total",
			Help:      "Client-side cache lookups via DoCache, by command verb and result (hit/miss).",
		}, []string{"command", "result"}),
	}
	for _, c := range []prometheus.Collector{m.duration, m.total, m.cache} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// observe records one command's duration and outcome.
func (m *Metrics) observe(command, status string, d time.Duration) {
	m.duration.WithLabelValues(command, status).Observe(d.Seconds())
	m.total.WithLabelValues(command, status).Inc()
}

// observeCache records a client-side cache hit or miss for a DoCache lookup.
func (m *Metrics) observeCache(command string, hit bool) {
	result := "miss"
	if hit {
		result = "hit"
	}
	m.cache.WithLabelValues(command, result).Inc()
}
