package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestGenerateIDsAreValidAndUnique(t *testing.T) {
	t1, t2 := GenerateTraceID(), GenerateTraceID()
	if !t1.IsValid() || !t2.IsValid() {
		t.Fatalf("generated trace ids must be valid")
	}
	if t1 == t2 {
		t.Fatalf("expected distinct trace ids")
	}
	s1, s2 := GenerateSpanID(), GenerateSpanID()
	if !s1.IsValid() || !s2.IsValid() {
		t.Fatalf("generated span ids must be valid")
	}
	if s1 == s2 {
		t.Fatalf("expected distinct span ids")
	}
}

func TestWithGeneratedSpanContext_AddsWhenAbsent(t *testing.T) {
	ctx := WithGeneratedSpanContext(context.Background())
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		t.Fatalf("expected a valid span context to be attached")
	}
	if !sc.IsSampled() {
		t.Fatalf("expected the generated span context to be sampled")
	}
}

func TestWithGeneratedSpanContext_PreservesExisting(t *testing.T) {
	tid, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	sid, _ := trace.SpanIDFromHex("0102030405060708")
	existing := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	ctx := trace.ContextWithSpanContext(context.Background(), existing)

	got := trace.SpanContextFromContext(WithGeneratedSpanContext(ctx))
	if got.TraceID() != tid {
		t.Fatalf("existing trace id must be preserved, got %s", got.TraceID())
	}
}
