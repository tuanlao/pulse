package snowflake

import (
	"context"
	"errors"
	"testing"
)

func TestParseOrdinal(t *testing.T) {
	tests := []struct {
		name    string
		pod     string
		want    int
		wantErr bool
	}{
		{"simple zero", "web-0", 0, false},
		{"nonzero", "web-3", 3, false},
		{"name with dashes", "my-app-12", 12, false},
		{"deployment random suffix", "web-5d4b9c-xk2lp", 0, true},
		{"no dash", "web", 0, true},
		{"trailing dash", "web-", 0, true},
		{"non-numeric suffix", "web-abc", 0, true},
		{"empty", "", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseOrdinal(tt.pod)
			if tt.wantErr {
				if !errors.Is(err, ErrNotStatefulSet) {
					t.Fatalf("parseOrdinal(%q) err = %v, want ErrNotStatefulSet", tt.pod, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOrdinal(%q) unexpected err: %v", tt.pod, err)
			}
			if got != tt.want {
				t.Fatalf("parseOrdinal(%q) = %d, want %d", tt.pod, got, tt.want)
			}
		})
	}
}

func TestStaticResolver(t *testing.T) {
	node, lease, err := staticResolver{id: 5}.resolve(context.Background(), 1023)
	if err != nil || node != 5 || lease != nil {
		t.Fatalf("static resolve = %d,%v,%v want 5,nil,nil", node, lease, err)
	}
	if _, _, err := (staticResolver{id: 2000}).resolve(context.Background(), 1023); !errors.Is(err, ErrWorkerIDOutOfRange) {
		t.Fatalf("expected ErrWorkerIDOutOfRange, got %v", err)
	}
	if _, _, err := (staticResolver{id: -1}).resolve(context.Background(), 1023); !errors.Is(err, ErrWorkerIDOutOfRange) {
		t.Fatalf("expected ErrWorkerIDOutOfRange for negative id, got %v", err)
	}
}

func TestStatefulSetResolver(t *testing.T) {
	t.Setenv("SNOWFLAKE_POD_NAME", "worker-7")
	r := statefulSetResolver{podNameEnv: "SNOWFLAKE_POD_NAME"}

	node, lease, err := r.resolve(context.Background(), 1023)
	if err != nil || node != 7 || lease != nil {
		t.Fatalf("statefulset resolve = %d,%v,%v want 7,nil,nil", node, lease, err)
	}

	// Ordinal exceeds the node space (maxNode 3 < 7).
	if _, _, err := r.resolve(context.Background(), 3); !errors.Is(err, ErrWorkerIDOutOfRange) {
		t.Fatalf("expected ErrWorkerIDOutOfRange, got %v", err)
	}

	// A Deployment-style name errors.
	t.Setenv("SNOWFLAKE_POD_NAME", "web-5d4b9c-xk2lp")
	if _, _, err := r.resolve(context.Background(), 1023); !errors.Is(err, ErrNotStatefulSet) {
		t.Fatalf("expected ErrNotStatefulSet for Deployment pod, got %v", err)
	}
}
