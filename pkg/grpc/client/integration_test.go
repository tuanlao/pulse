//go:build integration

// Integration tests for the pulse gRPC subsystem. They exercise the full
// server+client stack over a REAL localhost TCP listener: the built-in health
// service (unary Check + server-streaming Watch) plus a small hand-written test
// service (no protoc — it reuses the well-known wrapperspb/emptypb proto types)
// covering panic→Internal recovery, request timeouts, graceful drain, server
// restart/reconnect, trace propagation (tracer on + off) and TLS.
//
// Unlike the kafka/temporal integration tests these need NO external infra — the
// server runs in-process. Run with:
//
//	go test -race -tags=integration ./pkg/grpc/... -timeout 5m
//
// Set PULSE_GRPC_ADDR to point the client at an external server; tests that need
// to control the server (drain, restart, health flips, TLS) skip in that mode.
package client_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/tuanlao/pulse/pkg/grpc/client"
	"github.com/tuanlao/pulse/pkg/grpc/server"
	"github.com/tuanlao/pulse/pkg/grpc/server/interceptor"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestIntegration_HealthCheck_Unary(t *testing.T) {
	addr, _ := startServer(t, server.Deps{}, nil, nil)
	c := startClient(t, addr, client.Deps{})

	resp, err := grpc_health_v1.NewHealthClient(c.Conn()).Check(ctx2s(t), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.Status)
	}
}

func TestIntegration_HealthWatch_ServerStreaming(t *testing.T) {
	addr, srv := startServer(t, server.Deps{}, nil, nil)
	requireServer(t, srv)
	c := startClient(t, addr, client.Deps{})

	srv.SetServingStatus("watched", true)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	stream, err := grpc_health_v1.NewHealthClient(c.Conn()).Watch(ctx, &grpc_health_v1.HealthCheckRequest{Service: "watched"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if first.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("first status = %v, want SERVING", first.Status)
	}

	srv.SetServingStatus("watched", false)
	for {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("did not observe NOT_SERVING: %v", err)
		}
		if msg.Status == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			return
		}
	}
}

func TestIntegration_Echo_Unary(t *testing.T) {
	addr, _ := startServer(t, server.Deps{}, registerTest, nil)
	c := startClient(t, addr, client.Deps{})

	var out wrapperspb.StringValue
	if err := c.Conn().Invoke(ctx2s(t), echoMethod, wrapperspb.String("hi"), &out); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if out.Value != "hi" {
		t.Errorf("echo = %q, want hi", out.Value)
	}
}

func TestIntegration_Recovery_PanicReturnsInternal(t *testing.T) {
	addr, _ := startServer(t, server.Deps{}, registerTest, nil)
	c := startClient(t, addr, client.Deps{})

	var out wrapperspb.StringValue
	err := c.Conn().Invoke(ctx2s(t), panicMethod, &emptypb.Empty{}, &out)
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
}

func TestIntegration_RequestTimeout(t *testing.T) {
	addr, _ := startServer(t, server.Deps{}, registerTest, nil)
	c := startClient(t, addr, client.Deps{}, client.WithRequestTimeout(150*time.Millisecond))

	var out emptypb.Empty
	// No deadline on the caller ctx → the client applies Timeouts.Request; the
	// server sleeps far longer, so the call must hit DeadlineExceeded.
	err := c.Conn().Invoke(context.Background(), sleepMethod, wrapperspb.Int64(2000), &out)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Errorf("code = %v, want DeadlineExceeded", status.Code(err))
	}
}

func TestIntegration_GracefulDrain(t *testing.T) {
	addr, srv := startServer(t, server.Deps{}, registerTest, nil)
	requireServer(t, srv)
	c := startClient(t, addr, client.Deps{})

	errc := make(chan error, 1)
	go func() {
		var out emptypb.Empty
		errc <- c.Conn().Invoke(context.Background(), sleepMethod, wrapperspb.Int64(400), &out)
	}()
	// Let the RPC reach the server before draining.
	time.Sleep(100 * time.Millisecond)

	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-errc; err != nil {
		t.Errorf("in-flight RPC failed during graceful drain: %v", err)
	}
}

func TestIntegration_ServerRestart_ClientReconnects(t *testing.T) {
	if os.Getenv("PULSE_GRPC_ADDR") != "" {
		t.Skip("external server: restart not controllable")
	}
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	newServer := func() *server.Server {
		cfg := server.DefaultConfig()
		cfg.Port = port
		s, err := server.New(cfg, server.Deps{})
		if err != nil {
			t.Fatalf("server.New: %v", err)
		}
		s.Register(registerTest)
		if err := s.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		return s
	}

	s1 := newServer()
	c := startClient(t, addr, client.Deps{})

	if err := invokeEcho(ctx2s(t), c); err != nil {
		t.Fatalf("first echo: %v", err)
	}

	// Stop the server → an RPC now fails (retries exhaust quickly on Unavailable).
	_ = s1.Stop(context.Background())
	down, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := invokeEcho(down, c); err == nil {
		t.Fatal("expected an error while the server is down")
	}

	// Restart on the same address; the SAME client must reconnect and succeed.
	s2 := newServer()
	t.Cleanup(func() { _ = s2.Stop(context.Background()) })

	c.Conn().Connect() // reset the backoff so reconnection is prompt
	up, cancel2 := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel2()
	if err := invokeEcho(up, c, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("echo after restart: %v", err)
	}
}

func TestIntegration_TracePropagation_TracerOn(t *testing.T) {
	exp := sdktrace.NewSimpleSpanProcessor(noopExporter{})
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	addr, _ := startServer(t, server.Deps{TracerProvider: tp}, registerTest, nil)
	c := startClient(t, addr, client.Deps{TracerProvider: tp})

	ctx, span := tp.Tracer("test").Start(context.Background(), "caller")
	wantTrace := span.SpanContext().TraceID().String()

	var out wrapperspb.StringValue
	if err := c.Conn().Invoke(ctx, traceMethod, &emptypb.Empty{}, &out); err != nil {
		span.End()
		t.Fatalf("Invoke: %v", err)
	}
	span.End()

	// otelgrpc carries the trace via W3C traceparent; the server sees the SAME
	// trace id and an incoming traceparent header.
	if !strings.Contains(out.Value, "trace="+wantTrace) {
		t.Errorf("server-seen trace id mismatch: %q (want trace=%s)", out.Value, wantTrace)
	}
	if !strings.Contains(out.Value, "tp=00-"+wantTrace+"-") {
		t.Errorf("server did not receive traceparent: %q", out.Value)
	}
}

func TestIntegration_TracePropagation_TracerOff(t *testing.T) {
	addr, _ := startServer(t, server.Deps{}, registerTest, nil)
	c := startClient(t, addr, client.Deps{}) // no tracer → manual x-trace-id path

	tid := mustTrace(t, "0102030405060708090a0b0c0d0e0f10")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: tid, SpanID: mustSpan(t, "0102030405060708"), TraceFlags: trace.FlagsSampled,
	}))

	var out wrapperspb.StringValue
	if err := c.Conn().Invoke(ctx, traceMethod, &emptypb.Empty{}, &out); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	// The tracing-off path emits ONLY x-trace-id (no W3C traceparent).
	if !strings.Contains(out.Value, "xt="+tid.String()) {
		t.Errorf("x-trace-id not propagated: %q (want xt=%s)", out.Value, tid.String())
	}
	if !strings.Contains(out.Value, "tp=;") {
		t.Errorf("traceparent should be empty on the tracing-off path: %q", out.Value)
	}
}

func TestIntegration_Metrics_Recorded(t *testing.T) {
	reg := prometheus.NewRegistry()
	sm, err := interceptor.NewMetrics(interceptor.DefaultMetricsConfig(), reg)
	if err != nil {
		t.Fatalf("server NewMetrics: %v", err)
	}
	cm, err := client.NewMetrics(client.DefaultConfig().Metrics, reg)
	if err != nil {
		t.Fatalf("client NewMetrics: %v", err)
	}

	addr, _ := startServer(t, server.Deps{Metrics: sm}, registerTest, nil)
	c := startClient(t, addr, client.Deps{Metrics: cm})

	var out wrapperspb.StringValue
	if err := c.Conn().Invoke(ctx2s(t), echoMethod, wrapperspb.String("hi"), &out); err != nil {
		t.Fatalf("echo: %v", err)
	}
	_ = c.Conn().Invoke(ctx2s(t), panicMethod, &emptypb.Empty{}, &wrapperspb.StringValue{})

	if v := sampleValue(t, reg, "pulse_grpc_server_handled_total", map[string]string{
		"grpc_method": "Echo", "grpc_code": "OK",
	}); v < 1 {
		t.Errorf("server handled_total{Echo,OK} = %v, want >= 1", v)
	}
	if v := sampleValue(t, reg, "pulse_grpc_server_handled_total", map[string]string{
		"grpc_method": "Panic", "grpc_code": "Internal",
	}); v < 1 {
		t.Errorf("server handled_total{Panic,Internal} = %v, want >= 1", v)
	}
	if v := sampleValue(t, reg, "pulse_grpc_client_requests_total", map[string]string{
		"method": echoMethod, "code": "OK",
	}); v < 1 {
		t.Errorf("client requests_total{Echo,OK} = %v, want >= 1", v)
	}
}

func TestIntegration_TLS(t *testing.T) {
	if os.Getenv("PULSE_GRPC_ADDR") != "" {
		t.Skip("external server: TLS certs not controllable")
	}
	certFile, keyFile := genSelfSignedCert(t)

	addr, _ := startServer(t, server.Deps{}, registerTest, func(c *server.Config) {
		c.TLS = server.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile}
	})
	c := startClient(t, addr, client.Deps{}, client.WithTLS(client.TLSConfig{
		Enabled: true, CAFile: certFile, ServerNameOverride: "localhost",
	}))

	var out wrapperspb.StringValue
	if err := c.Conn().Invoke(ctx2s(t), echoMethod, wrapperspb.String("tls"), &out); err != nil {
		t.Fatalf("Invoke over TLS: %v", err)
	}
	if out.Value != "tls" {
		t.Errorf("echo = %q, want tls", out.Value)
	}
}

// --- helpers ----------------------------------------------------------------

// startServer builds and starts a server on a free localhost port (or returns the
// PULSE_GRPC_ADDR external address with a nil *Server). register runs before
// Start; mut tweaks the config (e.g. TLS).
func startServer(t *testing.T, deps server.Deps, register func(*grpc.Server), mut func(*server.Config)) (string, *server.Server) {
	t.Helper()
	if ext := os.Getenv("PULSE_GRPC_ADDR"); ext != "" {
		return ext, nil
	}
	port := freePort(t)
	cfg := server.DefaultConfig()
	cfg.Port = port
	if mut != nil {
		mut(&cfg)
	}
	srv, err := server.New(cfg, deps)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if register != nil {
		srv.Register(register)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return fmt.Sprintf("127.0.0.1:%d", port), srv
}

func startClient(t *testing.T, target string, deps client.Deps, opts ...client.Option) *client.Client {
	t.Helper()
	all := append([]client.Option{client.WithTarget(target)}, opts...)
	c, err := client.New(client.DefaultConfig(), deps, all...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("client.Start: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c
}

func requireServer(t *testing.T, srv *server.Server) {
	t.Helper()
	if srv == nil {
		t.Skip("requires an in-process server (PULSE_GRPC_ADDR is set)")
	}
}

func invokeEcho(ctx context.Context, c *client.Client, opts ...grpc.CallOption) error {
	var out wrapperspb.StringValue
	return c.Conn().Invoke(ctx, echoMethod, wrapperspb.String("hi"), &out, opts...)
}

func ctx2s(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func mustTrace(t *testing.T, hex string) trace.TraceID {
	t.Helper()
	id, err := trace.TraceIDFromHex(hex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	return id
}

func mustSpan(t *testing.T, hex string) trace.SpanID {
	t.Helper()
	id, err := trace.SpanIDFromHex(hex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	return id
}

// sampleValue returns the value of the metric named name whose labels include all
// of want; -1 if no such sample exists.
func sampleValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if labelsMatch(m.GetLabel(), want) {
				switch {
				case m.Counter != nil:
					return m.Counter.GetValue()
				case m.Gauge != nil:
					return m.Gauge.GetValue()
				case m.Histogram != nil:
					return float64(m.Histogram.GetSampleCount())
				}
			}
		}
	}
	return -1
}

func labelsMatch(have []*dto.LabelPair, want map[string]string) bool {
	got := make(map[string]string, len(have))
	for _, lp := range have {
		got[lp.GetName()] = lp.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func genSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")

	writePEM(t, certFile, "CERTIFICATE", der)
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

// --- hand-written test service (no protoc; uses well-known proto types) ------

const (
	echoMethod  = "/pulse.test.TestService/Echo"
	panicMethod = "/pulse.test.TestService/Panic"
	sleepMethod = "/pulse.test.TestService/Sleep"
	traceMethod = "/pulse.test.TestService/TraceProbe"
)

type testService interface {
	Echo(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
	Panic(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error)
	Sleep(context.Context, *wrapperspb.Int64Value) (*emptypb.Empty, error)
	TraceProbe(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error)
}

type testSvc struct{}

func (testSvc) Echo(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String(in.GetValue()), nil
}

func (testSvc) Panic(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error) {
	panic("boom in handler")
}

func (testSvc) Sleep(ctx context.Context, ms *wrapperspb.Int64Value) (*emptypb.Empty, error) {
	select {
	case <-time.After(time.Duration(ms.GetValue()) * time.Millisecond):
		return &emptypb.Empty{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (testSvc) TraceProbe(ctx context.Context, _ *emptypb.Empty) (*wrapperspb.StringValue, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	sc := trace.SpanContextFromContext(ctx)
	return wrapperspb.String(fmt.Sprintf("trace=%s;tp=%s;xt=%s",
		sc.TraceID().String(), firstMD(md, "traceparent"), firstMD(md, "x-trace-id"))), nil
}

func firstMD(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func unaryTestHandler[T any](method string, call func(testService, context.Context, *T) (any, error)) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
		in := new(T)
		if err := dec(in); err != nil {
			return nil, err
		}
		if ic == nil {
			return call(srv.(testService), ctx, in)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: method}
		h := func(ctx context.Context, req any) (any, error) {
			return call(srv.(testService), ctx, req.(*T))
		}
		return ic(ctx, in, info, h)
	}
}

var testServiceDesc = grpc.ServiceDesc{
	ServiceName: "pulse.test.TestService",
	HandlerType: (*testService)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Echo", Handler: unaryTestHandler(echoMethod, func(s testService, ctx context.Context, in *wrapperspb.StringValue) (any, error) {
			return s.Echo(ctx, in)
		})},
		{MethodName: "Panic", Handler: unaryTestHandler(panicMethod, func(s testService, ctx context.Context, in *emptypb.Empty) (any, error) {
			return s.Panic(ctx, in)
		})},
		{MethodName: "Sleep", Handler: unaryTestHandler(sleepMethod, func(s testService, ctx context.Context, in *wrapperspb.Int64Value) (any, error) {
			return s.Sleep(ctx, in)
		})},
		{MethodName: "TraceProbe", Handler: unaryTestHandler(traceMethod, func(s testService, ctx context.Context, in *emptypb.Empty) (any, error) {
			return s.TraceProbe(ctx, in)
		})},
	},
	Metadata: "pulse/test",
}

func registerTest(s *grpc.Server) { s.RegisterService(&testServiceDesc, testSvc{}) }

// noopExporter is a span exporter that discards spans (the trace test only needs
// a real, non-noop TracerProvider so the client/server take the otelgrpc path).
type noopExporter struct{}

func (noopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (noopExporter) Shutdown(context.Context) error                             { return nil }
