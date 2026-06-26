package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/log"
)

func init() { gin.SetMode(gin.TestMode) }

func TestContextLogger_StoresRequestLogger(t *testing.T) {
	base := log.Nop()
	r := gin.New()
	r.Use(ContextLogger(base))

	var hadLogger bool
	r.GET("/", func(c *gin.Context) {
		l := log.FromContext(c.Request.Context(), nil)
		hadLogger = l != nil
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	if !hadLogger {
		t.Fatalf("expected request-scoped logger in context")
	}
}

func TestRecovery_Returns500AndAborts(t *testing.T) {
	r := gin.New()
	r.Use(Recovery(log.Nop()))
	r.GET("/boom", func(c *gin.Context) { panic("kaboom") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestCORS_DisabledIsPassthrough(t *testing.T) {
	cfg := DefaultCORSConfig() // disabled
	r := gin.New()
	r.Use(CORS(cfg))
	r.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("expected no CORS headers when disabled")
	}
}

func TestCORS_PreflightAllowed(t *testing.T) {
	cfg := DefaultCORSConfig()
	cfg.Enabled = true
	r := gin.New()
	r.Use(CORS(cfg))
	r.POST("/", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for preflight, got %d", w.Code)
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("expected allow-origin *, got %q", w.Header().Get("Access-Control-Allow-Origin"))
	}
	if w.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Fatalf("expected allow-methods header on preflight")
	}
}

func TestCORS_CredentialsReflectsOrigin(t *testing.T) {
	cfg := DefaultCORSConfig()
	cfg.Enabled = true
	cfg.AllowCredentials = true
	r := gin.New()
	r.Use(CORS(cfg))
	r.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	r.ServeHTTP(w, req)

	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://example.com" {
		t.Fatalf("expected reflected origin with credentials, got %q", got)
	}
	if w.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("expected allow-credentials true")
	}
}

func TestBodyLimit_RejectsOversized(t *testing.T) {
	r := gin.New()
	r.Use(BodyLimit(8))
	r.POST("/", func(c *gin.Context) {
		_, err := c.GetRawData()
		if err != nil {
			c.Status(http.StatusRequestEntityTooLarge)
			return
		}
		c.Status(http.StatusOK)
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("0123456789abcdef"))
	r.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}
