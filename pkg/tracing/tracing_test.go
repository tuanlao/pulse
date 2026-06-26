package tracing

import (
	"context"
	"sync"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// recordingExporter is a SpanExporter that keeps exported spans even after
// Shutdown. (tracetest.InMemoryExporter resets on Shutdown, which hides spans
// flushed by TracerProvider.Shutdown, so it can't be used to assert flushing.)
type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *recordingExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *recordingExporter) Shutdown(context.Context) error { return nil }

func (e *recordingExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func (e *recordingExporter) names() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, 0, len(e.spans))
	for _, s := range e.spans {
		out = append(out, s.Name())
	}
	return out
}

func TestNew_DisabledUsesNoopProvider(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = false

	tr, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A no-op provider yields non-recording spans.
	_, span := tr.Provider().Tracer("test").Start(context.Background(), "op")
	if span.IsRecording() {
		t.Fatalf("expected non-recording span from no-op provider")
	}
	if err := tr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := tr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestNew_EnabledRequiresServiceName(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.ServiceName = ""
	if _, err := New(context.Background(), cfg); err == nil {
		t.Fatalf("expected error when ServiceName empty")
	}
}

func TestShutdownFlushesSpans(t *testing.T) {
	exp := &recordingExporter{}
	cfg := DefaultConfig()
	cfg.ServiceName = "svc-test"

	tr := newSDKTracer(cfg, exp)
	if err := tr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	_, span := tr.Provider().Tracer("t").Start(context.Background(), "op")
	span.End()

	// Spans are batched; nothing exported until shutdown flushes.
	if exp.count() != 0 {
		t.Fatalf("expected spans buffered before shutdown, got %d", exp.count())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := tr.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if exp.count() != 1 {
		t.Fatalf("expected 1 flushed span, got %d", exp.count())
	}
	if names := exp.names(); names[0] != "op" {
		t.Fatalf("unexpected span name %q", names[0])
	}
}

func TestProtocolSelection(t *testing.T) {
	// Exporters dial lazily; New must construct without a live collector for
	// both protocols.
	for _, p := range []Protocol{ProtocolGRPC, ProtocolHTTP} {
		cfg := DefaultConfig()
		cfg.ServiceName = "svc"
		cfg.Endpoint = "localhost:4317" // non-empty so the exporter is actually built
		cfg.Protocol = p
		tr, err := New(context.Background(), cfg)
		if err != nil {
			t.Fatalf("protocol %s: New: %v", p, err)
		}
		if _, ok := tr.Provider().(*sdktrace.TracerProvider); !ok {
			t.Fatalf("protocol %s: expected SDK provider", p)
		}
		_ = tr.Stop(context.Background())
	}
}

func TestNew_EmptyEndpointGeneratesIDsWithoutExport(t *testing.T) {
	// Endpoint "" means: tracing on (real SDK provider, so spans carry a valid
	// trace_id/span_id for log correlation) but no OTLP exporter is built.
	cfg := DefaultConfig()
	cfg.ServiceName = "svc"
	cfg.Endpoint = ""

	tr, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := tr.Provider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("expected a real SDK provider, got %T", tr.Provider())
	}

	_, span := tr.Provider().Tracer("t").Start(context.Background(), "op")
	if !span.IsRecording() {
		t.Fatalf("expected a recording span from the SDK provider")
	}
	if sc := span.SpanContext(); !sc.IsValid() || !sc.HasTraceID() || !sc.HasSpanID() {
		t.Fatalf("expected a valid span context with trace/span ids, got %+v", sc)
	}
	span.End()

	if err := tr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestComponentName(t *testing.T) {
	tr, _ := New(context.Background(), Config{Enabled: false})
	if tr.Name() != "tracing" {
		t.Fatalf("Name = %q, want tracing", tr.Name())
	}
}

func TestApplyDefaults_SampleRatio(t *testing.T) {
	cases := []struct {
		name string
		in   float64
		want float64
	}{
		{"zero is honored (never sample)", 0, 0},
		{"negative is treated as unset and backfilled", -1, DefaultConfig().SampleRatio},
		{"in-range value is kept", 0.25, 0.25},
		{"above one is clamped", 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{SampleRatio: tc.in}
			c.applyDefaults()
			if c.SampleRatio != tc.want {
				t.Fatalf("SampleRatio = %v, want %v", c.SampleRatio, tc.want)
			}
		})
	}
}

var _ trace.TracerProvider = (*sdktrace.TracerProvider)(nil)
