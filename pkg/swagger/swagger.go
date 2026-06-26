// Package swagger mounts a Swagger UI (swaggo/gin-swagger) onto a gin engine.
// It is DISABLED by default — production services should not expose their API
// surface unless they opt in. The route mount is gated by config; the generated
// docs package (from `swag init`) is still a compile-time import in the
// application.
//
// Typical application usage:
//
//	//go:generate swag init -g cmd/server/main.go -o docs
//	import _ "yourmodule/docs" // registers the generated swag spec
//	swagger.Mount(srv.Engine(), swagger.DefaultConfig(), swagger.WithEnabled(true))
package swagger

import (
	"strings"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
)

// Config configures the Swagger UI mount.
type Config struct {
	// Enabled toggles mounting the UI. Default false.
	Enabled bool `mapstructure:"enabled"`
	// Path is the base route for the UI. Default "/swagger".
	Path string `mapstructure:"path"`
	// Host is the API host shown in the spec, e.g. "api.example.com". Informational
	// here; the application applies it to its generated SwaggerInfo.
	Host string `mapstructure:"host"`
	// BasePath is the API base path shown in the spec, e.g. "/api/v1".
	BasePath string `mapstructure:"base_path"`
	// DocURL overrides the URL of the doc.json the UI fetches. Default is
	// "<Path>/doc.json".
	DocURL string `mapstructure:"doc_url"`
	// InstanceName selects which registered swag spec to serve. Default
	// "swagger".
	InstanceName string `mapstructure:"instance_name"`
	// DefaultModelsExpandDepth controls model expansion; -1 hides the models
	// section. Default 1.
	DefaultModelsExpandDepth int `mapstructure:"default_models_expand_depth"`
	// PersistAuthorization keeps auth between page reloads. Default false.
	PersistAuthorization bool `mapstructure:"persist_authorization"`
}

// DefaultConfig returns Config with sensible defaults (UI disabled).
func DefaultConfig() Config {
	return Config{
		Enabled:                  false,
		Path:                     "/swagger",
		BasePath:                 "/",
		InstanceName:             "swagger",
		DefaultModelsExpandDepth: 1,
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Path == "" {
		c.Path = d.Path
	}
	if c.InstanceName == "" {
		c.InstanceName = d.InstanceName
	}
	if c.BasePath == "" {
		c.BasePath = d.BasePath
	}
	if c.DefaultModelsExpandDepth == 0 {
		c.DefaultModelsExpandDepth = d.DefaultModelsExpandDepth
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithEnabled toggles the UI.
func WithEnabled(enabled bool) Option { return func(c *Config) { c.Enabled = enabled } }

// WithPath sets the base route.
func WithPath(path string) Option { return func(c *Config) { c.Path = path } }

// WithHost sets the API host.
func WithHost(host string) Option { return func(c *Config) { c.Host = host } }

// Mount registers the Swagger UI on engine when cfg.Enabled is true. When
// disabled it is a no-op (the route is never created, so it returns 404).
// It returns true when the UI was mounted.
func Mount(engine *gin.Engine, cfg Config, opts ...Option) bool {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults()

	if !cfg.Enabled {
		return false
	}

	base := "/" + strings.Trim(cfg.Path, "/")
	docURL := cfg.DocURL
	if docURL == "" {
		docURL = base + "/doc.json"
	}

	handler := ginSwagger.WrapHandler(
		swaggerFiles.Handler,
		ginSwagger.URL(docURL),
		ginSwagger.InstanceName(cfg.InstanceName),
		ginSwagger.DefaultModelsExpandDepth(cfg.DefaultModelsExpandDepth),
		ginSwagger.PersistAuthorization(cfg.PersistAuthorization),
	)

	engine.GET(base+"/*any", handler)
	return true
}
