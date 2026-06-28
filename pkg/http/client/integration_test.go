//go:build integration

// Integration tests driving a REAL loopback HTTP server (pkg/http/server) with the
// outbound client (pkg/http/client): client retry/backoff, timeouts, the server's
// body-limit, readiness gating, panic recovery, graceful drain, and end-to-end
// trace-id propagation. These need no docker.
package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/tracing"
)

func boolPtr(b bool) *bool { return &b }

// traceIDFromParent extracts the trace id from a W3C traceparent
// ("00-<trace-id>-<span-id>-<flags>").
func traceIDFromParent(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) >= 2 {
		return parts[1]
	}
	return ""
}

func freeHTTPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// startServer builds a server on a free port, registers routes, starts it, and
// returns the server and its base URL.
func startServer(t *testing.T, deps server.Deps, cfgMut func(*server.Config), register func(*gin.Engine)) (*server.Server, string) {
	t.Helper()
	port := freeHTTPPort(t)
	cfg := server.DefaultConfig()
	cfg.Mode = "test"
	cfg.Port = port
	if cfgMut != nil {
		cfgMut(&cfg)
	}
	srv, err := server.New(cfg, deps)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if register != nil {
		register(srv.Engine())
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server.Start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return srv, fmt.Sprintf("http://127.0.0.1:%d", port)
}

func newClient(t *testing.T, baseURL string, mut func(*client.Config), deps client.Deps) *client.Client {
	t.Helper()
	cfg := client.DefaultConfig()
	cfg.BaseURL = baseURL
	if mut != nil {
		mut(&cfg)
	}
	c, err := client.New(cfg, deps)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return c
}

func statusOf(err error) int {
	if err == nil {
		return 200
	}
	var he *client.HTTPError
	if errors.As(err, &he) {
		return he.StatusCode
	}
	return -1
}

func TestIntegration_HTTPClient_RetriesOn5xx(t *testing.T) {
	var hits atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer up.Close()

	c := newClient(t, up.URL, nil, client.Deps{})
	var out map[string]bool
	if err := c.GetJSON(context.Background(), "/", &out); err != nil {
		t.Fatalf("GetJSON should succeed after retries: %v", err)
	}
	if n := hits.Load(); n != 3 {
		t.Fatalf("upstream hit %d times, want 3 (2 retries then success)", n)
	}
}

func TestIntegration_HTTPClient_NoRetry(t *testing.T) {
	t.Run("get_4xx", func(t *testing.T) {
		var hits atomic.Int32
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer up.Close()
		c := newClient(t, up.URL, nil, client.Deps{})
		if err := c.GetJSON(context.Background(), "/", nil); statusOf(err) != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", statusOf(err))
		}
		if n := hits.Load(); n != 1 {
			t.Fatalf("4xx must not be retried, upstream hit %d times", n)
		}
	})

	t.Run("post_5xx_not_idempotent", func(t *testing.T) {
		var hits atomic.Int32
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer up.Close()
		c := newClient(t, up.URL, nil, client.Deps{})
		_ = c.PostJSON(context.Background(), "/", map[string]int{"a": 1}, nil)
		if n := hits.Load(); n != 1 {
			t.Fatalf("POST is not idempotent and must not be retried, upstream hit %d times", n)
		}
	})
}

func TestIntegration_HTTPClient_RequestTimeout(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()
	c := newClient(t, up.URL, func(cfg *client.Config) {
		cfg.Timeouts.Request = 300 * time.Millisecond
		cfg.Retry.Enabled = boolPtr(false)
	}, client.Deps{})
	err := c.GetJSON(context.Background(), "/", nil)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
}

func TestIntegration_HTTPServer_BodyLimit413(t *testing.T) {
	_, base := startServer(t, server.Deps{},
		func(c *server.Config) { c.MaxBodyBytes = 1024 },
		func(e *gin.Engine) {
			e.POST("/echo", func(gc *gin.Context) {
				if _, err := io.ReadAll(gc.Request.Body); err != nil {
					gc.Status(http.StatusRequestEntityTooLarge)
					return
				}
				gc.Status(http.StatusOK)
			})
		})

	c := newClient(t, base, func(cfg *client.Config) { cfg.Retry.Enabled = boolPtr(false) }, client.Deps{})
	big := map[string]string{"blob": strings.Repeat("x", 8192)}
	err := c.PostJSON(context.Background(), "/echo", big, nil)
	if statusOf(err) != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 for an over-limit body", statusOf(err))
	}
}

func TestIntegration_HTTPServer_ReadinessGating(t *testing.T) {
	var healthy atomic.Bool
	srv, base := startServer(t, server.Deps{}, nil, nil)
	srv.Readiness().Register("dep", func(context.Context) error {
		if healthy.Load() {
			return nil
		}
		return errors.New("not ready")
	})
	c := newClient(t, base, func(cfg *client.Config) { cfg.Retry.Enabled = boolPtr(false) }, client.Deps{})

	if got := statusOf(c.GetJSON(context.Background(), "/readyz", nil)); got != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy /readyz = %d, want 503", got)
	}
	healthy.Store(true)
	if got := statusOf(c.GetJSON(context.Background(), "/readyz", nil)); got != http.StatusOK {
		t.Fatalf("healthy /readyz = %d, want 200", got)
	}
}

func TestIntegration_HTTPServer_RecoversPanic(t *testing.T) {
	_, base := startServer(t, server.Deps{}, nil, func(e *gin.Engine) {
		e.GET("/boom", func(*gin.Context) { panic("kaboom") })
	})
	c := newClient(t, base, func(cfg *client.Config) { cfg.Retry.Enabled = boolPtr(false) }, client.Deps{})
	if got := statusOf(c.GetJSON(context.Background(), "/boom", nil)); got != http.StatusInternalServerError {
		t.Fatalf("panicking handler status = %d, want 500", got)
	}
}

// TestIntegration_HTTPClient_TracePropagation: the outbound client propagates the
// caller's trace context — the upstream sees the SAME W3C trace id as the active
// span on the request context.
func TestIntegration_HTTPClient_TracePropagation(t *testing.T) {
	tcfg := tracing.DefaultConfig()
	tcfg.ServiceName = "it-trace"
	tracer, err := tracing.New(context.Background(), tcfg)
	if err != nil {
		t.Fatalf("tracing.New: %v", err)
	}
	t.Cleanup(func() { _ = tracer.Stop(context.Background()) })
	tp := tracer.Provider()

	// Upstream echoes the W3C trace id it received.
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"trace_id":%q}`, traceIDFromParent(r.Header.Get("traceparent")))))
	}))
	defer up.Close()

	c := newClient(t, up.URL, nil, client.Deps{TracerProvider: tp})

	ctx, span := tp.Tracer("it").Start(context.Background(), "root")
	defer span.End()
	want := span.SpanContext().TraceID().String()

	var out struct {
		TraceID string `json:"trace_id"`
	}
	if err := c.GetJSON(ctx, "/", &out); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if out.TraceID != want {
		t.Fatalf("client did not propagate the trace id: upstream saw %q, want %q", out.TraceID, want)
	}
}

func TestIntegration_HTTPServer_GracefulDrain(t *testing.T) {
	port := freeHTTPPort(t)
	cfg := server.DefaultConfig()
	cfg.Mode = "test"
	srv, err := server.New(cfg, server.Deps{}, server.WithPort(port))
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.Engine().GET("/slow", func(gc *gin.Context) {
		time.Sleep(500 * time.Millisecond)
		gc.JSON(http.StatusOK, gin.H{"ok": true})
	})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("server.Start: %v", err)
	}

	c := newClient(t, fmt.Sprintf("http://127.0.0.1:%d", port), nil, client.Deps{})
	done := make(chan error, 1)
	go func() {
		var out map[string]bool
		done <- c.GetJSON(context.Background(), "/slow", &out)
	}()
	time.Sleep(100 * time.Millisecond) // ensure the request is in flight

	if err := srv.Stop(context.Background()); err != nil {
		t.Fatalf("graceful Stop: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("in-flight request should drain successfully, got: %v", err)
	}
}
