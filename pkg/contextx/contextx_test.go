package contextx

import (
	"context"
	"testing"
)

func TestLogger(t *testing.T) {
	ctx := context.Background()

	if got := Logger(ctx); got != nil {
		t.Fatalf("expected nil logger on empty context, got %v", got)
	}

	type fakeLogger struct{ name string }
	want := &fakeLogger{name: "req"}
	ctx = WithLogger(ctx, want)

	got, ok := Logger(ctx).(*fakeLogger)
	if !ok {
		t.Fatalf("stored logger did not round-trip to its concrete type")
	}
	if got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
