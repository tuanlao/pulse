package log

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// newObserved builds a Logger whose entries are captured by an observer core,
// using the package's encoder config indirectly via field names.
func newObserved(t *testing.T, cfg Config) (*Logger, *observer.ObservedLogs) {
	t.Helper()
	cfg.applyDefaults()
	core, logs := observer.New(zapcore.DebugLevel)
	return &Logger{z: zap.New(core), cfg: cfg}, logs
}

func TestForContext_AddsTraceFields(t *testing.T) {
	l, logs := newObserved(t, DefaultConfig())

	// Build a context with a valid OTel span context.
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	l.ForContext(ctx).Info("hello")

	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["trace_id"] != tid.String() {
		t.Fatalf("missing/incorrect trace_id field: %v", fields)
	}
	if fields["span_id"] != sid.String() {
		t.Fatalf("missing/incorrect span_id field: %v", fields)
	}
}

func TestForContext_CustomFieldNames(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TraceField = "tid"
	cfg.SpanField = "sid"
	l, logs := newObserved(t, cfg)

	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	l.ForContext(ctx).Info("msg")

	fields := logs.All()[0].ContextMap()
	if _, ok := fields["tid"]; !ok {
		t.Fatalf("custom trace field not used: %v", fields)
	}
	if _, ok := fields["sid"]; !ok {
		t.Fatalf("custom span field not used: %v", fields)
	}
}

func TestForContext_NoContextValuesReturnsSameLogger(t *testing.T) {
	l, _ := newObserved(t, DefaultConfig())
	if got := l.ForContext(context.Background()); got != l {
		t.Fatalf("expected same logger when no context values present")
	}
}

func TestFromContext_Fallback(t *testing.T) {
	fallback, _ := newObserved(t, DefaultConfig())

	// No logger in context -> fallback returned.
	if got := FromContext(context.Background(), fallback); got != fallback {
		t.Fatalf("expected fallback logger")
	}

	// Logger in context -> that logger returned.
	req, _ := newObserved(t, DefaultConfig())
	ctx := IntoContext(context.Background(), req)
	if got := FromContext(ctx, fallback); got != req {
		t.Fatalf("expected request-scoped logger from context")
	}

	// Nil fallback -> never nil.
	if got := FromContext(context.Background(), nil); got == nil {
		t.Fatalf("FromContext returned nil")
	}
}

func TestNew_ConsoleFormatHasBrackets(t *testing.T) {
	// Build a real console logger writing to an in-memory buffer via OutputPaths
	// is awkward; instead assert the encoder config produces bracketed output by
	// encoding a single entry.
	ec := encoderConfig("console")
	enc := zapcore.NewConsoleEncoder(ec)
	buf, err := enc.EncodeEntry(zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Message: "hello",
	}, nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out := buf.String()
	if got := out; got == "" {
		t.Fatalf("empty output")
	}
	if !contains(out, "[INFO]") {
		t.Fatalf("expected [INFO] in console output, got %q", out)
	}
}

func TestNew_InvalidLevel(t *testing.T) {
	if _, err := New(Config{Level: "not-a-level"}); err == nil {
		t.Fatalf("expected error for invalid level")
	}
}

func TestNew_Defaults(t *testing.T) {
	l, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if l.Config().Encoding != "console" {
		t.Fatalf("expected console encoding default")
	}
	// Sync must not error on stderr sinks.
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync returned error: %v", err)
	}
}

func TestLifecycleAdapter(t *testing.T) {
	l, logs := newObserved(t, DefaultConfig())
	adapter := l.LifecycleAdapter()
	adapter.Info("started", "component", "http")
	adapter.Error("failed", "component", "db", "error", context.Canceled)

	all := logs.All()
	if len(all) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(all))
	}
	if all[0].ContextMap()["component"] != "http" {
		t.Fatalf("adapter did not map fields: %v", all[0].ContextMap())
	}
}

func TestGocronAdapter(t *testing.T) {
	l, logs := newObserved(t, DefaultConfig())
	a := l.GocronAdapter()
	a.Debug("debug", "k", "v")
	a.Info("info", "n", "1")
	a.Warn("warn")
	a.Error("boom", "error", context.Canceled)

	all := logs.All()
	if len(all) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(all))
	}
	// An error value is attached as a named error field, not a generic Any.
	if got := all[3].ContextMap()["error"]; got != context.Canceled.Error() {
		t.Fatalf("error field = %v, want %q", got, context.Canceled.Error())
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
