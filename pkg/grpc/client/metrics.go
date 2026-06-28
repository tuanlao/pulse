package client

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ClientMetrics holds the outbound gRPC RED collectors. Like the kafka/http
// metrics it registers into a provided registry (or a fresh package-owned one),
// never the global default. Labels are bounded: target is the configured dial
// target, method is the static full method, grpc_type is the streaming kind, and
// code is the finite gRPC status code set.
type ClientMetrics struct {
	reg      *prometheus.Registry
	duration *prometheus.HistogramVec
	total    *prometheus.CounterVec
}

var grpcClientLabels = []string{"target", "method", "grpc_type", "code"}

// NewMetrics builds the outbound collectors and registers them into reg (or a new
// registry if reg is nil).
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
			Help:      "Outbound gRPC request latency in seconds, by target/method/type/code.",
			Buckets:   cfg.Buckets,
		}, grpcClientLabels),
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "requests_total",
			Help:      "Total outbound gRPC requests, by target/method/type/code.",
		}, grpcClientLabels),
	}
	for _, c := range []prometheus.Collector{m.duration, m.total} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in. It is intended
// for exposing the metrics (scrape), not for runtime Gather.
func (m *ClientMetrics) Registry() *prometheus.Registry { return m.reg }

// observe records one logical request (all gRPC retries collapsed). It is nil-safe.
func (m *ClientMetrics) observe(target, method, rpcType, code string, d time.Duration) {
	if m == nil {
		return
	}
	m.duration.WithLabelValues(target, method, rpcType, code).Observe(d.Seconds())
	m.total.WithLabelValues(target, method, rpcType, code).Inc()
}
