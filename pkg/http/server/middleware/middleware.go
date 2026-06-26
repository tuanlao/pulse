// Package middleware provides the gin middleware that pulse's HTTP server wires
// together: panic recovery, request-scoped logging, CORS, and body-size
// limiting. RED metrics live in pkg/metrics and OTel span instrumentation comes
// from otelgin; both are wired by pkg/httpx.
package middleware

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tuanlao/pulse/pkg/log"
	"go.uber.org/zap"
)

// ContextLogger builds a request-scoped logger carrying (when tracing is active)
// the OTel trace/span ids, then stores it in the request context for handlers to
// retrieve via log.FromContext. It must run after the otelgin span middleware so
// the trace/span ids are available.
func ContextLogger(base *log.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		reqLogger := base.ForContext(ctx)
		c.Request = c.Request.WithContext(log.IntoContext(ctx, reqLogger))
		c.Next()
	}
}

// Recovery catches panics from inner handlers/middleware, logs them with the
// request-scoped logger (falling back to base), and responds 500 if nothing has
// been written yet. It is the outermost middleware.
func Recovery(base *log.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				l := log.FromContext(c.Request.Context(), base)
				l.Error("panic recovered",
					zap.Any("panic", rec),
					zap.ByteString("stack", debug.Stack()),
				)
				if !c.Writer.Written() {
					c.AbortWithStatus(http.StatusInternalServerError)
				} else {
					c.Abort()
				}
			}
		}()
		c.Next()
	}
}

// CORSConfig configures the CORS middleware.
type CORSConfig struct {
	// Enabled toggles CORS handling. Default false.
	Enabled bool `mapstructure:"enabled"`
	// AllowOrigins is the list of allowed origins. "*" allows any. Default ["*"].
	AllowOrigins []string `mapstructure:"allow_origins"`
	// AllowMethods is the list of allowed methods.
	AllowMethods []string `mapstructure:"allow_methods"`
	// AllowHeaders is the list of allowed request headers.
	AllowHeaders []string `mapstructure:"allow_headers"`
	// ExposeHeaders is the list of response headers exposed to the browser.
	ExposeHeaders []string `mapstructure:"expose_headers"`
	// AllowCredentials sets Access-Control-Allow-Credentials.
	AllowCredentials bool `mapstructure:"allow_credentials"`
	// MaxAgeSeconds sets Access-Control-Max-Age for preflight caching.
	MaxAgeSeconds int `mapstructure:"max_age_seconds"`
}

// DefaultCORSConfig returns a permissive-by-default-but-disabled CORS config.
func DefaultCORSConfig() CORSConfig {
	return CORSConfig{
		Enabled:       false,
		AllowOrigins:  []string{"*"},
		AllowMethods:  []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:  []string{"Origin", "Content-Type", "Accept", "Authorization"},
		MaxAgeSeconds: 600,
	}
}

// CORS returns a CORS middleware. When cfg.Enabled is false it is a pass-through.
func CORS(cfg CORSConfig) gin.HandlerFunc {
	allowAll := false
	originSet := make(map[string]struct{}, len(cfg.AllowOrigins))
	for _, o := range cfg.AllowOrigins {
		if o == "*" {
			allowAll = true
		}
		originSet[o] = struct{}{}
	}
	// Static header values are derived once from config, not per request.
	exposeHeaders := strings.Join(cfg.ExposeHeaders, ", ")
	allowMethods := strings.Join(cfg.AllowMethods, ", ")
	allowHeaders := strings.Join(cfg.AllowHeaders, ", ")
	maxAge := strconv.Itoa(cfg.MaxAgeSeconds)

	return func(c *gin.Context) {
		if !cfg.Enabled {
			c.Next()
			return
		}
		origin := c.GetHeader("Origin")
		if origin != "" {
			if allowAll {
				if cfg.AllowCredentials {
					// Reflect the origin when credentials are allowed ("*" is
					// invalid with credentials per the CORS spec).
					c.Header("Access-Control-Allow-Origin", origin)
					c.Header("Vary", "Origin")
				} else {
					c.Header("Access-Control-Allow-Origin", "*")
				}
			} else if _, ok := originSet[origin]; ok {
				c.Header("Access-Control-Allow-Origin", origin)
				c.Header("Vary", "Origin")
			}
		}
		if cfg.AllowCredentials {
			c.Header("Access-Control-Allow-Credentials", "true")
		}
		if exposeHeaders != "" {
			c.Header("Access-Control-Expose-Headers", exposeHeaders)
		}

		if c.Request.Method == http.MethodOptions {
			c.Header("Access-Control-Allow-Methods", allowMethods)
			c.Header("Access-Control-Allow-Headers", allowHeaders)
			if cfg.MaxAgeSeconds > 0 {
				c.Header("Access-Control-Max-Age", maxAge)
			}
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}

// BodyLimit caps the request body size. A request whose body exceeds maxBytes
// will fail when handlers read past the limit (via http.MaxBytesReader). A
// non-positive maxBytes disables the limit.
func BodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if maxBytes > 0 {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		}
		c.Next()
	}
}
