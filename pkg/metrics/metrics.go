// Package metrics provides Prometheus instrumentation for HTTP services using
// the RED method (Rate, Errors, Duration). It uses the native
// prometheus/client_golang library with a package-owned registry (never the
// global default registry and never the OTel->Prometheus bridge) so collectors,
// buckets and labels are fully under control and tests stay hermetic.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Config configures the metrics subsystem.
type Config struct {
	// Enabled toggles metrics collection and the /metrics endpoint. Default true.
	Enabled bool `mapstructure:"enabled"`
	// Path is the scrape endpoint. Default "/metrics".
	Path string `mapstructure:"path"`
	// Namespace is the Prometheus metric namespace. Default "pulse".
	Namespace string `mapstructure:"namespace"`
	// Subsystem is the Prometheus metric subsystem. Default "http".
	Subsystem string `mapstructure:"subsystem"`
	// DurationBuckets are histogram buckets (seconds) for request duration.
	// Default is an SLO-friendly set.
	DurationBuckets []float64 `mapstructure:"duration_buckets"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:   true,
		Path:      "/metrics",
		Namespace: "pulse",
		Subsystem: "http",
		DurationBuckets: []float64{
			0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
		},
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Path == "" {
		c.Path = d.Path
	}
	if c.Namespace == "" {
		c.Namespace = d.Namespace
	}
	if c.Subsystem == "" {
		c.Subsystem = d.Subsystem
	}
	if len(c.DurationBuckets) == 0 {
		c.DurationBuckets = d.DurationBuckets
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithPath sets the scrape endpoint path.
func WithPath(path string) Option { return func(c *Config) { c.Path = path } }

// WithNamespace sets the metric namespace.
func WithNamespace(ns string) Option { return func(c *Config) { c.Namespace = ns } }

// WithBuckets sets the request-duration histogram buckets.
func WithBuckets(buckets []float64) Option { return func(c *Config) { c.DurationBuckets = buckets } }

// RED holds the RED collectors and the package-owned registry.
type RED struct {
	cfg         Config
	reg         *prometheus.Registry
	reqDuration *prometheus.HistogramVec
	reqTotal    *prometheus.CounterVec
	inFlight    prometheus.Gauge
}

var redLabels = []string{"method", "route", "status"}

// New creates a RED instrument set registered into a fresh, package-owned
// registry that also includes the Go runtime and process collectors.
func New(cfg Config, opts ...Option) (*RED, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	reg := prometheus.NewRegistry()

	m := &RED{
		cfg: cfg,
		reg: reg,
		reqDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "request_duration_seconds",
			Help:      "HTTP request latency in seconds, by method/route/status.",
			Buckets:   cfg.DurationBuckets,
		}, redLabels),
		reqTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "requests_total",
			Help:      "Total HTTP requests, by method/route/status.",
		}, redLabels),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.Namespace,
			Subsystem: cfg.Subsystem,
			Name:      "requests_in_flight",
			Help:      "In-flight HTTP requests.",
		}),
	}

	for _, c := range []prometheus.Collector{
		m.reqDuration, m.reqTotal, m.inFlight,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Config returns the resolved configuration.
func (m *RED) Config() Config { return m.cfg }

// Registry returns the package-owned Prometheus registry.
func (m *RED) Registry() *prometheus.Registry { return m.reg }

// Handler returns an http.Handler that serves the registry in the Prometheus
// exposition format.
func (m *RED) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// Middleware returns a gin middleware that records RED metrics. The route label
// uses the gin route PATTERN (c.FullPath(), e.g. "/users/:id"), never the raw
// path, to keep cardinality bounded. Unmatched routes (404) collapse to a single
// "unmatched" series. Paths in skip are excluded entirely (e.g. /metrics,
// /healthz, /readyz).
func (m *RED) Middleware(skip ...string) gin.HandlerFunc {
	skipSet := make(map[string]struct{}, len(skip))
	for _, s := range skip {
		skipSet[s] = struct{}{}
	}

	return func(c *gin.Context) {
		if _, ok := skipSet[c.Request.URL.Path]; ok {
			c.Next()
			return
		}

		start := time.Now()
		m.inFlight.Inc()

		// Observe in a defer so the request is still counted when an inner
		// handler panics. On panic the status is forced to 500 and the panic is
		// re-raised so the outer recovery middleware can log it and write the
		// response. This keeps recovery outermost while RED accounting stays
		// correct.
		defer func() {
			m.inFlight.Dec()
			rec := recover()

			status := c.Writer.Status()
			if rec != nil {
				status = http.StatusInternalServerError
			}

			route := c.FullPath()
			if route == "" {
				route = "unmatched"
			}
			method := c.Request.Method
			dur := time.Since(start).Seconds()

			m.reqDuration.WithLabelValues(method, route, strconv.Itoa(status)).Observe(dur)
			m.reqTotal.WithLabelValues(method, route, strconv.Itoa(status)).Inc()

			if rec != nil {
				panic(rec)
			}
		}()

		c.Next() // run the handler; FullPath and status are resolved afterwards.
	}
}
