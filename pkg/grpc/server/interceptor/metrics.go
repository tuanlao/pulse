package interceptor

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetricsConfig configures the gRPC server RED metrics.
type MetricsConfig struct {
	// Enabled toggles metrics. When false, NewMetrics returns nil. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Namespace is the Prometheus namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus subsystem. Default "grpc_server".
	Subsystem string `mapstructure:"subsystem"`
	// Buckets are the handler duration histogram buckets (seconds).
	Buckets []float64 `mapstructure:"buckets"`
}

// DefaultMetricsConfig returns the metrics defaults.
func DefaultMetricsConfig() MetricsConfig {
	return MetricsConfig{
		Enabled:   true,
		Namespace: "pulse",
		Subsystem: "grpc_server",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}
}

// ApplyDefaults fills empty namespace/subsystem/buckets from d.
func (c *MetricsConfig) ApplyDefaults(d MetricsConfig) {
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

// grpcServerLabels bound metric cardinality to static proto-derived values plus
// the finite gRPC status code set: service+method come from the proto, grpc_type
// is the streaming kind, grpc_code is codes.Code.String().
var grpcServerLabels = []string{"grpc_service", "grpc_method", "grpc_type", "grpc_code"}

// inFlightLabels omit the code (an in-flight RPC has no code yet).
var inFlightLabels = []string{"grpc_service", "grpc_method", "grpc_type"}

// Metrics holds the gRPC server RED collectors.
type Metrics struct {
	reg      *prometheus.Registry
	duration *prometheus.HistogramVec // service, method, type, code
	total    *prometheus.CounterVec   // service, method, type, code
	inFlight *prometheus.GaugeVec     // service, method, type
}

// NewMetrics builds the collectors and registers them into reg (or a fresh
// registry when reg is nil). It honors cfg.Enabled: when disabled it returns a
// nil *Metrics so wiring the result into server.Deps truly disables metrics
// rather than being a no-op flag (kafka semantics).
func NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*Metrics, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg.ApplyDefaults(DefaultMetricsConfig())
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	ns, sub := cfg.Namespace, cfg.Subsystem

	m := &Metrics{
		reg: reg,
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "handler_duration_seconds",
			Help:    "gRPC server handler latency in seconds, by service/method/type/code.",
			Buckets: cfg.Buckets,
		}, grpcServerLabels),
		total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "handled_total",
			Help: "Total gRPC requests handled, by service/method/type/code.",
		}, grpcServerLabels),
		inFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "in_flight",
			Help: "In-flight gRPC requests, by service/method/type.",
		}, inFlightLabels),
	}
	for _, c := range []prometheus.Collector{m.duration, m.total, m.inFlight} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in. It is intended
// for exposing the metrics (scrape), not for runtime Gather.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Unary returns the metrics unary server interceptor. It is nil-safe.
//
// The recording runs in a defer so a panicking handler is still accounted for
// (as codes.Internal — matching what the outer Recovery interceptor returns),
// then the panic is re-raised for Recovery to convert and log. This mirrors the
// HTTP RED middleware, which records a 500 in its defer and re-panics.
func (m *Metrics) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		service, method := splitFullMethod(info.FullMethod)
		m.inc(service, method, "unary")
		start := time.Now()
		defer func() {
			rec := recover()
			code := status.Code(err).String()
			if rec != nil {
				code = codes.Internal.String()
			}
			m.dec(service, method, "unary")
			m.observe(service, method, "unary", code, time.Since(start))
			if rec != nil {
				panic(rec)
			}
		}()
		resp, err = handler(ctx, req)
		return resp, err
	}
}

// Stream returns the metrics stream server interceptor. It is nil-safe.
func (m *Metrics) Stream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		service, method := splitFullMethod(info.FullMethod)
		typ := streamType(info)
		m.inc(service, method, typ)
		start := time.Now()
		defer func() {
			rec := recover()
			code := status.Code(err).String()
			if rec != nil {
				code = codes.Internal.String()
			}
			m.dec(service, method, typ)
			m.observe(service, method, typ, code, time.Since(start))
			if rec != nil {
				panic(rec)
			}
		}()
		err = handler(srv, ss)
		return err
	}
}

func (m *Metrics) inc(service, method, typ string) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(service, method, typ).Inc()
}

func (m *Metrics) dec(service, method, typ string) {
	if m == nil {
		return
	}
	m.inFlight.WithLabelValues(service, method, typ).Dec()
}

func (m *Metrics) observe(service, method, typ, code string, d time.Duration) {
	if m == nil {
		return
	}
	m.duration.WithLabelValues(service, method, typ, code).Observe(d.Seconds())
	m.total.WithLabelValues(service, method, typ, code).Inc()
}
