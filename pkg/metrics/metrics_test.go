package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func init() { gin.SetMode(gin.TestMode) }

func newRED(t *testing.T) *RED {
	t.Helper()
	m, err := New(DefaultConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return m
}

func TestMiddleware_UsesRoutePattern(t *testing.T) {
	m := newRED(t)
	r := gin.New()
	r.Use(m.Middleware())
	r.GET("/users/:id", func(c *gin.Context) { c.Status(http.StatusOK) })

	// Two different ids must collapse into ONE series labelled /users/:id.
	for _, id := range []string{"1", "2", "3"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/users/"+id, nil)
		r.ServeHTTP(w, req)
	}

	got := testutil.ToFloat64(m.reqTotal.WithLabelValues("GET", "/users/:id", "200"))
	if got != 3 {
		t.Fatalf("expected 3 requests on /users/:id series, got %v", got)
	}
}

func TestMiddleware_UnmatchedCollapses(t *testing.T) {
	m := newRED(t)
	r := gin.New()
	r.Use(m.Middleware())
	r.GET("/known", func(c *gin.Context) { c.Status(http.StatusOK) })

	for _, p := range []string{"/nope/a", "/nope/b"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, p, nil)
		r.ServeHTTP(w, req)
	}

	got := testutil.ToFloat64(m.reqTotal.WithLabelValues("GET", "unmatched", "404"))
	if got != 2 {
		t.Fatalf("expected 2 unmatched requests collapsed, got %v", got)
	}
}

func TestMiddleware_PanicRecordedAs500(t *testing.T) {
	m := newRED(t)
	r := gin.New()
	// Outer recovery that mimics the httpx recovery middleware.
	r.Use(func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				c.AbortWithStatus(http.StatusInternalServerError)
			}
		}()
		c.Next()
	})
	r.Use(m.Middleware())
	r.GET("/boom", func(c *gin.Context) { panic("kaboom") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))

	got := testutil.ToFloat64(m.reqTotal.WithLabelValues("GET", "/boom", "500"))
	if got != 1 {
		t.Fatalf("expected panic recorded as 500, got %v", got)
	}
}

func TestMiddleware_SkipPaths(t *testing.T) {
	m := newRED(t)
	r := gin.New()
	r.Use(m.Middleware("/metrics"))
	r.GET("/metrics", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.ServeHTTP(w, req)

	if c := testutil.CollectAndCount(m.reqTotal); c != 0 {
		t.Fatalf("expected skipped path to record no series, got %d", c)
	}
}

func TestHandler_ServesExposition(t *testing.T) {
	m := newRED(t)
	r := gin.New()
	r.Use(m.Middleware(m.Config().Path))
	r.GET(m.Config().Path, gin.WrapH(m.Handler()))
	r.GET("/ping", func(c *gin.Context) { c.Status(http.StatusOK) })

	// Generate one observation.
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ping", nil))

	// Scrape.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "pulse_http_request_duration_seconds") {
		t.Fatalf("exposition missing duration metric:\n%s", body)
	}
	if !strings.Contains(body, "pulse_http_requests_total") {
		t.Fatalf("exposition missing counter metric")
	}
}

func TestNew_OwnRegistryNoCrossContamination(t *testing.T) {
	// Two independent RED instances must not collide (own registries).
	if _, err := New(DefaultConfig()); err != nil {
		t.Fatalf("first New: %v", err)
	}
	if _, err := New(DefaultConfig()); err != nil {
		t.Fatalf("second New must not double-register: %v", err)
	}
}

func TestOptionsOverride(t *testing.T) {
	m, err := New(DefaultConfig(), WithPath("/custommetrics"), WithNamespace("svc"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.Config().Path != "/custommetrics" || m.Config().Namespace != "svc" {
		t.Fatalf("options not applied: %+v", m.Config())
	}
}
