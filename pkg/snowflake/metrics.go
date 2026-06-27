package snowflake

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds the package-owned Prometheus collectors. Following pulse's
// registry discipline it registers into a provided registry (so it can share the
// server's /metrics endpoint) or a fresh package-owned one — never the global
// default registry. Cardinality is bounded: there are no per-id labels.
type Metrics struct {
	reg          *prometheus.Registry
	generated    prometheus.Counter
	workerID     prometheus.Gauge
	clockBack    prometheus.Counter
	seqExhausted prometheus.Counter
	leaseLost    prometheus.Counter
	fenced       prometheus.Gauge
}

// NewMetrics builds the collectors and registers them into reg (or a fresh
// registry when reg is nil). It returns (nil, nil) when disabled, so the caller
// wires the result into Deps.Metrics and the generator skips every metric call.
func NewMetrics(cfg MetricsConfig, reg *prometheus.Registry) (*Metrics, error) {
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
	if reg == nil {
		reg = prometheus.NewRegistry()
	}

	m := &Metrics{
		reg: reg,
		generated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "ids_generated_total", Help: "Total snowflake ids generated.",
		}),
		workerID: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "worker_id", Help: "The resolved worker (node) id of this generator.",
		}),
		clockBack: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "clock_backwards_total", Help: "Times the wall clock moved backwards during Generate.",
		}),
		seqExhausted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "sequence_exhausted_total", Help: "Times the per-millisecond sequence overflowed and Generate spun to the next millisecond.",
		}),
		leaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "worker_id_lease_lost_total", Help: "Times the redis worker-id lease was lost and the generator fenced.",
		}),
		fenced: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: cfg.Namespace, Subsystem: cfg.Subsystem,
			Name: "fenced", Help: "1 while the generator is fenced (refusing to generate after losing its redis lease), else 0.",
		}),
	}
	for _, c := range []prometheus.Collector{
		m.generated, m.workerID, m.clockBack, m.seqExhausted, m.leaseLost, m.fenced,
	} {
		if err := reg.Register(c); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Registry returns the registry the collectors are registered in.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// The mutators are nil-safe so call sites need no nil checks: a nil *Metrics
// (metrics disabled / not wired) makes every update a no-op.

func (m *Metrics) incGenerated() {
	if m != nil {
		m.generated.Inc()
	}
}

func (m *Metrics) incClockBackwards() {
	if m != nil {
		m.clockBack.Inc()
	}
}

func (m *Metrics) incSeqExhausted() {
	if m != nil {
		m.seqExhausted.Inc()
	}
}

func (m *Metrics) incLeaseLost() {
	if m != nil {
		m.leaseLost.Inc()
	}
}

func (m *Metrics) setWorkerID(id int64) {
	if m != nil {
		m.workerID.Set(float64(id))
	}
}

func (m *Metrics) setFenced(fenced bool) {
	if m == nil {
		return
	}
	if fenced {
		m.fenced.Set(1)
		return
	}
	m.fenced.Set(0)
}
