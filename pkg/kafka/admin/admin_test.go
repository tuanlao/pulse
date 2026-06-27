package admin

import (
	"testing"
)

func TestUnique(t *testing.T) {
	in := []string{"a", "b", "a", "", "c", "b"}
	got := unique(in)
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("unique = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("unique[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	c.ApplyDefaults()
	if !c.AutoCreate {
		t.Error("AutoCreate should default true")
	}
	if c.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", c.Partitions)
	}
	if c.ReplicationFactor != 1 {
		t.Errorf("ReplicationFactor = %d, want 1", c.ReplicationFactor)
	}
}
