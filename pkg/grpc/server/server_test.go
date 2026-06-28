package server

import (
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
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
	if c.Port != DefaultConfig().Port {
		t.Errorf("Port = %d, want %d", c.Port, DefaultConfig().Port)
	}
	// EnableHealth is a default-true bool: it is only set by DefaultConfig(), not
	// re-applied to a zero-value Config (the documented caveat).
	if !DefaultConfig().EnableHealth {
		t.Error("DefaultConfig().EnableHealth should be true")
	}
}

func TestConfig_OptionsOverride(t *testing.T) {
	c := DefaultConfig()
	for _, o := range []Option{
		WithPort(1234), WithReflection(true), WithHealth(false),
		WithMaxRecvMsgSize(11), WithMaxSendMsgSize(22),
	} {
		o(&c)
	}
	if c.Port != 1234 || !c.EnableReflection || c.EnableHealth || c.MaxRecvMsgSize != 11 || c.MaxSendMsgSize != 22 {
		t.Errorf("options not applied: %+v", c)
	}
}

func TestServer_Name(t *testing.T) {
	srv, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv.Name() != "grpc" {
		t.Errorf("Name() = %q, want grpc", srv.Name())
	}
}

func TestServer_New_HealthRegistered(t *testing.T) {
	_, conn := bufServer(t)
	resp, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.Status)
	}
}

func TestServer_New_ReflectionOptIn(t *testing.T) {
	off, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if hasReflection(off.Server().GetServiceInfo()) {
		t.Error("reflection registered when disabled")
	}
	on, err := New(DefaultConfig(), Deps{}, WithReflection(true))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !hasReflection(on.Server().GetServiceInfo()) {
		t.Error("reflection not registered when enabled")
	}
}

func TestServer_CheckReady(t *testing.T) {
	srv, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.CheckReady(context.Background()); err == nil {
		t.Error("CheckReady before Start = nil, want error")
	}
	lis := bufconn.Listen(1 << 20)
	if err := srv.serve(lis); err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	if err := srv.CheckReady(context.Background()); err != nil {
		t.Errorf("CheckReady after Start = %v, want nil", err)
	}
}

func TestServer_StartStopGraceful(t *testing.T) {
	srv, err := New(DefaultConfig(), Deps{}, WithPort(freePort(t)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestServer_Register_PanicsAfterStart(t *testing.T) {
	srv, err := New(DefaultConfig(), Deps{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	if err := srv.serve(lis); err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	defer func() {
		if recover() == nil {
			t.Error("Register after Start did not panic")
		}
	}()
	srv.Register(func(*grpc.Server) {})
}

func TestServer_HealthWatch_NotServingOnStop(t *testing.T) {
	srv, conn := bufServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := grpc_health_v1.NewHealthClient(conn).Watch(ctx, &grpc_health_v1.HealthCheckRequest{})
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

	go func() { _ = srv.Stop(context.Background()) }()
	for {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("did not observe NOT_SERVING before stream ended: %v", err)
		}
		if msg.Status == grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			return
		}
	}
}

// --- test helpers -----------------------------------------------------------

// bufServer builds a server serving over an in-memory bufconn listener and a
// client conn dialed through it. Both are torn down via t.Cleanup.
func bufServer(t *testing.T, opts ...Option) (*Server, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv, err := New(DefaultConfig(), Deps{}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := srv.serve(lis); err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return srv, conn
}

func hasReflection(info map[string]grpc.ServiceInfo) bool {
	for name := range info {
		if strings.Contains(name, "ServerReflection") {
			return true
		}
	}
	return false
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
