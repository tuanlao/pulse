// Package metrics holds the package-owned Prometheus collectors for pkg/kafka.
// Following pulse's registry discipline, NewMetrics registers into a provided
// registry (typically the server's, so /metrics exposes kafka metrics alongside
// the rest) — never the global default. Label cardinality is bounded to
// topic/group/status/class (never keys, partitions, or message ids).
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Config configures the kafka Prometheus metrics.
type Config struct {
	// Enabled toggles metrics. When false, NewMetrics returns nil. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "kafka".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the handler/produce duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// DefaultConfig returns the metrics defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:   true,
		Namespace: "pulse",
		Subsystem: "kafka",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
	}
}

// ApplyDefaults fills empty namespace/subsystem/buckets from d.
func (c *Config) ApplyDefaults(d Config) {
	if c.Namespace == "" {
		c.Namespace = d.Namespace
	}
	if c.Subsystem == "" {
		c.Subsystem = d.Subsystem
	}
	if len(c.Buckets) == 0 {
		c.Buckets = d.Buckets
	}
}

// Metrics holds the kafka collectors.
type Metrics struct {
	reg *prometheus.Registry

	produceTotal    *prometheus.CounterVec   // topic, status
	produceDuration *prometheus.HistogramVec // topic, status
	consumeTotal    *prometheus.CounterVec   // topic, group, status
	consumeDuration *prometheus.HistogramVec // topic, group
	retriesTotal    *prometheus.CounterVec   // topic, group
	dlqTotal        *prometheus.CounterVec   // topic, group, class
	dedupSkipped    *prometheus.CounterVec   // topic, group
	groupSkipped    *prometheus.CounterVec   // topic, group
	backoffPaused   *prometheus.GaugeVec     // topic, group
	inFlight        *prometheus.GaugeVec     // topic, group
}

// NewMetrics builds the collectors and registers them into reg (or a fresh
// registry when reg is nil). It honors cfg.Enabled: when disabled it returns a
// nil *Metrics so wiring the result into Deps truly disables metrics rather than
// being a no-op flag.
func NewMetrics(cfg Config, reg *prometheus.Registry) (*Metrics, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg.ApplyDefaults(DefaultConfig())
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	ns, sub := cfg.Namespace, cfg.Subsystem

	m := &Metrics{
		reg: reg,
		produceTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "produce_total",
			Help: "Total records produced, by topic and status (ok/error).",
		}, []string{"topic", "status"}),
		produceDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "produce_duration_seconds",
			Help: "Produce duration in seconds, by topic and status.", Buckets: cfg.Buckets,
		}, []string{"topic", "status"}),
		consumeTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "consume_total",
			Help: "Total records consumed, by topic, group and status (success/error/retry/dlq/dedup_skip/group_skip).",
		}, []string{"topic", "group", "status"}),
		consumeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "consume_duration_seconds",
			Help: "Handler duration in seconds, by topic and group.", Buckets: cfg.Buckets,
		}, []string{"topic", "group"}),
		retriesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "retries_total",
			Help: "Total records forwarded to a retry tier, by topic and group.",
		}, []string{"topic", "group"}),
		dlqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "dlq_total",
			Help: "Total records routed to the DLQ, by topic, group and error class.",
		}, []string{"topic", "group", "class"}),
		dedupSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "dedup_skipped_total",
			Help: "Total records skipped as duplicates, by topic and group.",
		}, []string{"topic", "group"}),
		groupSkipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "group_skipped_total",
			Help: "Total retry records skipped because scoped to another group, by topic and group.",
		}, []string{"topic", "group"}),
		backoffPaused: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "backoff_paused",
			Help: "Retry-tier partitions currently paused awaiting their due time, by topic and group.",
		}, []string{"topic", "group"}),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "handler_in_flight",
			Help: "Handlers currently executing, by topic and group.",
		}, []string{"topic", "group"}),
	}
	for _, c := range []prometheus.Collector{
		m.produceTotal, m.produceDuration, m.consumeTotal, m.consumeDuration,
		m.retriesTotal, m.dlqTotal, m.dedupSkipped, m.groupSkipped, m.backoffPaused, m.inFlight,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// All record methods are nil-safe so callers never branch on a disabled metrics
// set.

func (m *Metrics) ObserveProduce(topic, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.produceTotal.WithLabelValues(topic, status).Inc()
	m.produceDuration.WithLabelValues(topic, status).Observe(d.Seconds())
}

func (m *Metrics) ObserveConsume(topic, group, status string, d time.Duration) {
	if m == nil {
		return
	}
	m.consumeTotal.WithLabelValues(topic, group, status).Inc()
	if d > 0 {
		m.consumeDuration.WithLabelValues(topic, group).Observe(d.Seconds())
	}
}

func (m *Metrics) IncRetry(topic, group string) {
	if m == nil {
		return
	}
	m.retriesTotal.WithLabelValues(topic, group).Inc()
}

func (m *Metrics) IncDLQ(topic, group, class string) {
	if m == nil {
		return
	}
	m.dlqTotal.WithLabelValues(topic, group, class).Inc()
}

func (m *Metrics) IncDedupSkip(topic, group string) {
	if m == nil {
		return
	}
	m.dedupSkipped.WithLabelValues(topic, group).Inc()
}

func (m *Metrics) IncGroupSkip(topic, group string) {
	if m == nil {
		return
	}
	m.groupSkipped.WithLabelValues(topic, group).Inc()
}

func (m *Metrics) BackoffPaused(topic, group string, delta float64) {
	if m == nil {
		return
	}
	m.backoffPaused.WithLabelValues(topic, group).Add(delta)
}

func (m *Metrics) InFlight(topic, group string, delta float64) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(topic, group).Add(delta)
}
