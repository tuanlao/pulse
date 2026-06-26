package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
)

func init() { gin.SetMode(gin.TestMode) }

// freePort asks the OS for an unused TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func newServer(t *testing.T, opts ...Option) *Server {
	t.Helper()
	red, err := metrics.New(metrics.DefaultConfig())
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	s, err := New(DefaultConfig(), Deps{Logger: log.Nop(), Metrics: red}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestServer_Healthz(t *testing.T) {
	s := newServer(t)
	w := httptest.NewRecorder()
	s.Engine().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", w.Code)
	}
}

func TestServer_Readyz_NoChecksIsReady(t *testing.T) {
	s := newServer(t)
	w := httptest.NewRecorder()
	s.Engine().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/readyz with no checks = %d, want 200", w.Code)
	}
}

func TestServer_Readyz_FailingCheck(t *testing.T) {
	s := newServer(t)
	s.Readiness().Register("db", func(context.Context) error { return errors.New("not connected") })
	s.Readiness().Register("cache", func(context.Context) error { return nil })

	w := httptest.NewRecorder()
	s.Engine().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with failing check = %d, want 503", w.Code)
	}
}

func TestServer_PanicRecovered(t *testing.T) {
	s := newServer(t)
	s.Engine().GET("/boom", func(c *gin.Context) { panic("kaboom") })

	w := httptest.NewRecorder()
	s.Engine().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 after panic, got %d", w.Code)
	}
}

func TestServer_MetricsEndpoint(t *testing.T) {
	s := newServer(t)
	s.Engine().GET("/ping", func(c *gin.Context) { c.Status(http.StatusOK) })

	s.Engine().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))

	w := httptest.NewRecorder()
	s.Engine().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", w.Code)
	}
}

func TestServer_TimeoutsSet(t *testing.T) {
	s := newServer(t)
	if s.httpSrv.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout not set: %v", s.httpSrv.ReadHeaderTimeout)
	}
	if s.httpSrv.ReadTimeout == 0 || s.httpSrv.WriteTimeout == 0 || s.httpSrv.IdleTimeout == 0 {
		t.Fatalf("http.Server timeouts must all be set: %+v", s.httpSrv)
	}
}

func TestServer_StartStopGraceful(t *testing.T) {
	// Bind to a free port discovered at runtime.
	s := newServer(t, WithPort(freePort(t)))
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestServer_Name(t *testing.T) {
	if newServer(t).Name() != "http" {
		t.Fatalf("Name should be http")
	}
}

func TestReadiness_ContextIgnoringCheckDoesNotHang(t *testing.T) {
	reg := NewReadinessRegistry(50 * time.Millisecond)
	// A check that ignores its context and blocks must not wedge Evaluate.
	reg.Register("slow", func(context.Context) error {
		time.Sleep(2 * time.Second)
		return nil
	})

	start := time.Now()
	results, ready := reg.Evaluate(context.Background())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Evaluate hung for %v (perCheckTimeout was 50ms)", elapsed)
	}
	if ready {
		t.Fatalf("expected not-ready when a check times out")
	}
	if results["slow"].Status != "fail" {
		t.Fatalf("expected slow check to fail, got %+v", results["slow"])
	}
}
