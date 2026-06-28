package interceptor

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewMetrics_DisabledReturnsNil(t *testing.T) {
	cfg := DefaultMetricsConfig()
	cfg.Enabled = false
	m, err := NewMetrics(cfg, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if m != nil {
		t.Errorf("disabled metrics = %v, want nil", m)
	}
}

func TestNewMetrics_RegistersCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(DefaultMetricsConfig(), reg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	if m == nil {
		t.Fatal("NewMetrics returned nil")
	}
	m.observe("pkg.Svc", "M", "unary", "OK", time.Millisecond)
	if n := testutil.CollectAndCount(m.total); n != 1 {
		t.Errorf("series count = %d, want 1", n)
	}
}

func TestMetricsUnary_RecordsLabelsAndCode(t *testing.T) {
	m, err := NewMetrics(DefaultMetricsConfig(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	handler := func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "nope")
	}
	_, err = m.Unary()(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"}, handler)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
	if got := testutil.ToFloat64(m.total.WithLabelValues("pkg.Svc", "M", "unary", "NotFound")); got != 1 {
		t.Errorf("handled_total = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.inFlight.WithLabelValues("pkg.Svc", "M", "unary")); got != 0 {
		t.Errorf("in_flight = %v, want 0 (should return to baseline)", got)
	}
}

func TestMetricsStream_RecordsCode(t *testing.T) {
	m, err := NewMetrics(DefaultMetricsConfig(), prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	ss := &fakeServerStream{ctx: context.Background()}
	handler := func(any, grpc.ServerStream) error {
		return status.Error(codes.Unavailable, "down")
	}
	err = m.Stream()(nil, ss,
		&grpc.StreamServerInfo{FullMethod: "/pkg.Svc/S", IsServerStream: true}, handler)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable", status.Code(err))
	}
	if got := testutil.ToFloat64(m.total.WithLabelValues("pkg.Svc", "S", "server_stream", "Unavailable")); got != 1 {
		t.Errorf("handled_total = %v, want 1", got)
	}
}
