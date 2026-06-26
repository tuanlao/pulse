package swagger

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() { gin.SetMode(gin.TestMode) }

func TestMount_DisabledIsNoop(t *testing.T) {
	r := gin.New()
	mounted := Mount(r, DefaultConfig()) // disabled by default
	if mounted {
		t.Fatalf("expected Mount to report not mounted when disabled")
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when swagger disabled, got %d", w.Code)
	}
}

func TestMount_EnabledServesUI(t *testing.T) {
	r := gin.New()
	mounted := Mount(r, DefaultConfig(), WithEnabled(true))
	if !mounted {
		t.Fatalf("expected Mount to report mounted when enabled")
	}

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil))
	// The UI handler serves the page (200) or redirects to it; either way it is
	// not a 404.
	if w.Code == http.StatusNotFound {
		t.Fatalf("expected swagger UI to be served, got 404")
	}
}

func TestMount_CustomPath(t *testing.T) {
	r := gin.New()
	Mount(r, DefaultConfig(), WithEnabled(true), WithPath("/docs"))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs/index.html", nil))
	if w.Code == http.StatusNotFound {
		t.Fatalf("expected swagger UI at custom path, got 404")
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.Enabled {
		t.Fatalf("swagger must be disabled by default")
	}
	if c.Path != "/swagger" {
		t.Fatalf("default path = %q, want /swagger", c.Path)
	}
}
