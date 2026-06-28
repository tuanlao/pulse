package client

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestConfig_DefaultsIdempotent(t *testing.T) {
	var c Config
	c.applyDefaults()
	once := c
	c.applyDefaults()
	if !reflect.DeepEqual(once, c) {
		t.Errorf("applyDefaults not idempotent:\n once = %+v\n twice = %+v", once, c)
	}
}

func TestConfig_OptionsOverride(t *testing.T) {
	c := DefaultConfig()
	for _, o := range []Option{
		WithTarget("dns:///svc:9090"), WithRequestTimeout(time.Second),
		WithTraceIDHeader("x-corr"), WithUserAgent("ua"), WithServiceConfig(`{"x":1}`),
	} {
		o(&c)
	}
	if c.Target != "dns:///svc:9090" || c.Timeouts.Request != time.Second ||
		c.Trace.TraceIDHeader != "x-corr" || c.UserAgent != "ua" || c.Retry.RawServiceConfig != `{"x":1}` {
		t.Errorf("options not applied: %+v", c)
	}
}

func TestConfig_TriStateBools(t *testing.T) {
	var c Config
	c.applyDefaults()
	if !boolValue(c.Retry.Enabled) {
		t.Error("Retry.Enabled nil should backfill to true")
	}
	if !boolValue(c.Trace.Propagate) {
		t.Error("Trace.Propagate nil should backfill to true")
	}

	c2 := Config{Retry: RetryConfig{Enabled: boolPtr(false)}, Trace: TraceConfig{Propagate: boolPtr(false)}}
	c2.applyDefaults()
	if boolValue(c2.Retry.Enabled) {
		t.Error("explicit Retry.Enabled=false should be preserved")
	}
	if boolValue(c2.Trace.Propagate) {
		t.Error("explicit Trace.Propagate=false should be preserved")
	}
}

func TestServiceConfigJSON(t *testing.T) {
	disabled := DefaultConfig()
	disabled.Retry.Enabled = boolPtr(false)
	if got := disabled.serviceConfigJSON(); got != "" {
		t.Errorf("disabled JSON = %q, want empty", got)
	}

	enabled := DefaultConfig()
	js := enabled.serviceConfigJSON()
	if js == "" {
		t.Fatal("enabled JSON is empty")
	}
	var parsed struct {
		MethodConfig []struct {
			RetryPolicy struct {
				MaxAttempts          int      `json:"maxAttempts"`
				InitialBackoff       string   `json:"initialBackoff"`
				RetryableStatusCodes []string `json:"retryableStatusCodes"`
			} `json:"retryPolicy"`
		} `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(js), &parsed); err != nil {
		t.Fatalf("unmarshal: %v (%s)", err, js)
	}
	if len(parsed.MethodConfig) != 1 {
		t.Fatalf("methodConfig len = %d, want 1", len(parsed.MethodConfig))
	}
	rp := parsed.MethodConfig[0].RetryPolicy
	if rp.MaxAttempts != 3 {
		t.Errorf("maxAttempts = %d, want 3", rp.MaxAttempts)
	}
	if rp.InitialBackoff != "0.1s" {
		t.Errorf("initialBackoff = %q, want 0.1s", rp.InitialBackoff)
	}
	if len(rp.RetryableStatusCodes) != 1 || rp.RetryableStatusCodes[0] != "UNAVAILABLE" {
		t.Errorf("retryableStatusCodes = %v", rp.RetryableStatusCodes)
	}

	raw := DefaultConfig()
	raw.Retry.RawServiceConfig = `{"raw":true}`
	if got := raw.serviceConfigJSON(); got != `{"raw":true}` {
		t.Errorf("raw override = %q", got)
	}
}

func TestEnsureIDs_AddsSpanContext(t *testing.T) {
	ctx := ensureIDsCtx(context.Background())
	if sc := trace.SpanContextFromContext(ctx); !sc.IsValid() {
		t.Error("ensureIDsCtx did not add a valid span context")
	}

	tid := mustTraceID(t, "0102030405060708090a0b0c0d0e0f10")
	pre := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: mustSpanID(t, "0102030405060708"), TraceFlags: trace.FlagsSampled,
	}))
	if got := trace.SpanContextFromContext(ensureIDsCtx(pre)); got.TraceID() != tid {
		t.Error("ensureIDsCtx overwrote an existing span context")
	}
}

func TestManualTrace_InjectsMetadata(t *testing.T) {
	tid := mustTraceID(t, "0102030405060708090a0b0c0d0e0f10")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: mustSpanID(t, "0102030405060708"), TraceFlags: trace.FlagsSampled,
	}))

	var md metadata.MD
	invoker := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		md, _ = metadata.FromOutgoingContext(ctx)
		return nil
	}
	if err := correlationUnary(DefaultConfig().Trace)(ctx, "/pkg.Svc/M", nil, nil, nil, invoker); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if vals := md.Get("x-trace-id"); len(vals) != 1 || vals[0] != tid.String() {
		t.Errorf("x-trace-id = %v, want [%s]", vals, tid.String())
	}
	// The tracing-OFF path must NOT emit a W3C traceparent.
	if vals := md.Get("traceparent"); len(vals) != 0 {
		t.Errorf("traceparent should not be emitted on the tracing-off path, got %v", vals)
	}
}

func TestRequestTimeout_AppliesWhenNoDeadline(t *testing.T) {
	var hadDeadline bool
	probe := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		_, hadDeadline = ctx.Deadline()
		return nil
	}
	if err := requestTimeoutUnary(50*time.Millisecond)(context.Background(), "/m", nil, nil, nil, probe); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !hadDeadline {
		t.Error("expected a deadline to be applied when the caller had none")
	}

	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()
	want, _ := parent.Deadline()
	var got time.Time
	probe2 := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		got, _ = ctx.Deadline()
		return nil
	}
	if err := requestTimeoutUnary(50*time.Millisecond)(parent, "/m", nil, nil, nil, probe2); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("caller deadline overridden: got %v, want %v", got, want)
	}
}

func TestObsUnary_RecordsMetrics(t *testing.T) {
	m, err := NewMetrics(DefaultConfig().Metrics, prometheus.NewRegistry())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	cc, err := grpc.NewClient("passthrough:///obs-test", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	target := cc.Target()

	ok := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return nil }
	if err := obsUnary(m, nil)(context.Background(), "/pkg.Svc/M", nil, nil, cc, ok); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if got := testutil.ToFloat64(m.total.WithLabelValues(target, "/pkg.Svc/M", "unary", "OK")); got != 1 {
		t.Errorf("OK total = %v, want 1", got)
	}

	fail := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "down")
	}
	_ = obsUnary(m, nil)(context.Background(), "/pkg.Svc/M", nil, nil, cc, fail)
	if got := testutil.ToFloat64(m.total.WithLabelValues(target, "/pkg.Svc/M", "unary", "Unavailable")); got != 1 {
		t.Errorf("Unavailable total = %v, want 1", got)
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		return context.Canceled
	}
	_ = obsUnary(m, nil)(canceledCtx, "/pkg.Svc/M", nil, nil, cc, canceled)
	if got := testutil.ToFloat64(m.total.WithLabelValues(target, "/pkg.Svc/M", "unary", "canceled")); got != 1 {
		t.Errorf("canceled total = %v, want 1", got)
	}
}

func TestNewMetrics_RegistersCollectors(t *testing.T) {
	reg := prometheus.NewRegistry()
	m, err := NewMetrics(DefaultConfig().Metrics, reg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	m.observe("t", "/m", "unary", "OK", time.Millisecond)
	if n := testutil.CollectAndCount(m.total); n != 1 {
		t.Errorf("series count = %d, want 1", n)
	}
}

func TestClient_Name(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithTarget("passthrough:///x"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	if c.Name() != "grpc-client" {
		t.Errorf("Name() = %q, want grpc-client", c.Name())
	}
}

func TestClient_New_RequiresTarget(t *testing.T) {
	if _, err := New(DefaultConfig(), Deps{}); err == nil {
		t.Error("New with empty target = nil error, want error")
	}
}

func TestClient_CheckReady(t *testing.T) {
	c, err := New(DefaultConfig(), Deps{}, WithTarget("passthrough:///x"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A lazy (Idle) connection is usable, so CheckReady passes.
	if err := c.CheckReady(context.Background()); err != nil {
		t.Errorf("CheckReady (idle) = %v, want nil", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// A closed (Shutdown) connection is not ready.
	if err := c.CheckReady(context.Background()); err == nil {
		t.Error("CheckReady after Close = nil, want error")
	}
}

func TestClient_RoundTrip_Bufconn(t *testing.T) {
	lis, stop := bufHealthServer(t)
	defer stop()

	c, err := newClient(DefaultConfig(), Deps{},
		[]grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})},
		WithTarget("passthrough:///bufnet"))
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	resp, err := grpc_health_v1.NewHealthClient(c.Conn()).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.Status)
	}
}

// --- test helpers -----------------------------------------------------------

func bufHealthServer(t *testing.T) (*bufconn.Listener, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)
	go func() { _ = gs.Serve(lis) }()
	return lis, gs.Stop
}

func mustTraceID(t *testing.T, hex string) trace.TraceID {
	t.Helper()
	id, err := trace.TraceIDFromHex(hex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	return id
}

func mustSpanID(t *testing.T, hex string) trace.SpanID {
	t.Helper()
	id, err := trace.SpanIDFromHex(hex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	return id
}
