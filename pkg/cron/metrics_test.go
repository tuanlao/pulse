package cron

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics_DisabledReturnsNil(t *testing.T) {
	cfg := DefaultConfig().Metrics
	cfg.Enabled = false
	m, err := NewMetrics(cfg, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if m != nil {
		t.Fatalf("expected nil metrics when disabled, got %+v", m)
	}
}

func TestNewMetrics_EnabledBuildsCollectors(t *testing.T) {
	cfg := DefaultConfig().Metrics
	m, err := NewMetrics(cfg, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if m == nil {
		t.Fatalf("expected non-nil metrics when enabled")
	}
}
