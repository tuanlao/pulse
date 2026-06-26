package client

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ClientMetrics holds the outbound RED collectors. It lives here (not in
// pkg/metrics) because its shape differs from the server RED set: it labels by
// host/method/status (no gin route) and adds a retries counter. It still follows
// pulse's registry discipline — it registers into a provided registry (so it can
// share the server's /metrics endpoint) or a fresh package-owned one, never the
// global default registry.
type ClientMetrics struct {
	reg      *prometheus.Registry
	duration *prometheus.HistogramVec
	total    *prometheus.CounterVec
	retries  *prometheus.CounterVec
}

var clientLabels = []string{"host", "method", "status"}

// NewMetrics builds the outbound collectors and registers them into reg (or a
// new registry if reg is nil).
func NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*ClientMetrics, error) {
	cfg.applyDefaults(DefaultConfig().Metrics)
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &ClientMetrics{
		reg: reg,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "request_duration_seconds",
			Help:      "Outbound HTTP request latency in seconds, by host/method/status.",
			Buckets:   cfg.Buckets,
		}, clientLabels),
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "requests_total",
			Help:      "Total outbound HTTP requests, by host/method/status.",
		}, clientLabels),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "retries_total",
			Help:      "Total outbound HTTP retry attempts, by host/method.",
		}, []string{"host", "method"}),
	}
	for _, c := range []prometheus.Collector{m.duration, m.total, m.retries} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in.
func (m *ClientMetrics) Registry() *prometheus.Registry { return m.reg }

// observe records one logical request (all retries collapsed).
func (m *ClientMetrics) observe(host, method, status string, d time.Duration) {
	m.duration.WithLabelValues(host, method, status).Observe(d.Seconds())
	m.total.WithLabelValues(host, method, status).Inc()
}

// addRetry records a single retry attempt for host/method.
func (m *ClientMetrics) addRetry(host, method string) {
	m.retries.WithLabelValues(host, method).Inc()
}
