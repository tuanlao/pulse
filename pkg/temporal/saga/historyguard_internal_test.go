package saga

import "testing"

func TestDefaultThresholds(t *testing.T) {
	d := DefaultThresholds()
	if d.MaxEvents != defaultMaxEvents {
		t.Fatalf("MaxEvents = %d, want %d", d.MaxEvents, defaultMaxEvents)
	}
	if d.MaxBytes != defaultMaxBytes {
		t.Fatalf("MaxBytes = %d, want %d", d.MaxBytes, defaultMaxBytes)
	}
	if d.IgnoreServerSuggestion {
		t.Fatal("default should respect the server suggestion")
	}
}

func TestThresholdsWithDefaults(t *testing.T) {
	tests := []struct {
		name      string
		in        Thresholds
		wantEvent int
		wantBytes int
	}{
		{"zero fills both", Thresholds{}, defaultMaxEvents, defaultMaxBytes},
		{"negative fills both", Thresholds{MaxEvents: -1, MaxBytes: -1}, defaultMaxEvents, defaultMaxBytes},
		{"explicit kept", Thresholds{MaxEvents: 5, MaxBytes: 7}, 5, 7},
		{"partial fills missing", Thresholds{MaxEvents: 5}, 5, defaultMaxBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in.withDefaults()
			if got.MaxEvents != tc.wantEvent {
				t.Errorf("MaxEvents = %d, want %d", got.MaxEvents, tc.wantEvent)
			}
			if got.MaxBytes != tc.wantBytes {
				t.Errorf("MaxBytes = %d, want %d", got.MaxBytes, tc.wantBytes)
			}
		})
	}
}
