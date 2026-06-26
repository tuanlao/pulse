// Package app aggregates every pulse component's configuration into one
// AppConfig object (with a deployment Env), and loads it via pkg/config.
//
// This is a convenience aggregator: components remain independent and composable
// (you can still build each from its own DefaultConfig + Options). AppConfig just
// gives services a single nested-object config to load from YAML/env/flags and a
// place to express cross-cutting values like Env, ServiceName and Version.
package app

import (
	"errors"
	"strings"

	"github.com/tuanlao/pulse/pkg/config"
	"github.com/tuanlao/pulse/pkg/cron"
	"github.com/tuanlao/pulse/pkg/http/client"
	"github.com/tuanlao/pulse/pkg/http/server"
	"github.com/tuanlao/pulse/pkg/lifecycle"
	"github.com/tuanlao/pulse/pkg/log"
	"github.com/tuanlao/pulse/pkg/metrics"
	"github.com/tuanlao/pulse/pkg/swagger"
	"github.com/tuanlao/pulse/pkg/tracing"
)

// Env is the deployment environment.
type Env string

const (
	EnvLocal Env = "LOCAL"
	EnvDev   Env = "DEV"
	EnvUAT   Env = "UAT"
	EnvStg   Env = "STG"
	EnvProd  Env = "PROD"
)

// Valid reports whether e is a known environment (case-insensitive). Unlike
// normalized(), it does not fall back to a default — an unknown value is invalid.
func (e Env) Valid() bool {
	switch Env(strings.ToUpper(strings.TrimSpace(string(e)))) {
	case EnvLocal, EnvDev, EnvUAT, EnvStg, EnvProd:
		return true
	default:
		return false
	}
}

// IsProd reports whether e is the production environment.
func (e Env) IsProd() bool { return e.normalized() == EnvProd }

// normalized upper-cases e; unknown/empty values become EnvDev.
func (e Env) normalized() Env {
	switch Env(strings.ToUpper(strings.TrimSpace(string(e)))) {
	case EnvLocal:
		return EnvLocal
	case EnvUAT:
		return EnvUAT
	case EnvStg:
		return EnvStg
	case EnvProd:
		return EnvProd
	case EnvDev:
		return EnvDev
	default:
		return EnvDev
	}
}

// Config is the unified application config (AppConfig). It is nested-object
// shaped so it maps onto structured YAML (each component is its own object).
type Config struct {
	Env         Env    `mapstructure:"env"`
	ServiceName string `mapstructure:"service_name"`
	Version     string `mapstructure:"version"`

	Log     log.Config     `mapstructure:"log"`
	Server  server.Config  `mapstructure:"server"`
	Tracing tracing.Config `mapstructure:"tracing"`
	Metrics metrics.Config `mapstructure:"metrics"`
	Swagger swagger.Config `mapstructure:"swagger"`
	Cron    cron.Config    `mapstructure:"cron"`

	// HttpClients holds named upstream HTTP clients (e.g. "payment", "user").
	// Named so future client types (grpc, redis, ...) can sit alongside it.
	HttpClients map[string]client.Config `mapstructure:"http_clients"`

	Lifecycle lifecycle.Config `mapstructure:"lifecycle"`
}

// DefaultConfig returns the composed defaults. Server.Mode is intentionally left
// empty so Normalize can derive it from Env (call Normalize, which Load does).
func DefaultConfig() Config {
	srv := server.DefaultConfig()
	srv.Mode = "" // let Env drive gin mode in Normalize

	return Config{
		Env:         EnvDev,
		Log:         log.DefaultConfig(),
		Server:      srv,
		Tracing:     tracing.DefaultConfig(),
		Metrics:     metrics.DefaultConfig(),
		Swagger:     swagger.DefaultConfig(),
		Cron:        cron.DefaultConfig(),
		HttpClients: map[string]client.Config{},
		Lifecycle:   lifecycle.DefaultConfig(),
	}
}

// Normalize propagates cross-cutting values into sub-configs and applies
// Env-derived defaults. It is idempotent and is called automatically by Load.
func (c *Config) Normalize() {
	c.Env = c.Env.normalized()

	if c.ServiceName != "" && c.Tracing.ServiceName == "" {
		c.Tracing.ServiceName = c.ServiceName
	}
	if c.Version != "" && c.Tracing.ServiceVersion == "" {
		c.Tracing.ServiceVersion = c.Version
	}
	if c.Tracing.Environment == "" {
		c.Tracing.Environment = string(c.Env)
	}
	if c.Server.Mode == "" {
		c.Server.Mode = ginModeForEnv(c.Env)
	}
}

// ginModeForEnv maps an Env to a gin mode: release for prod-like, debug for dev.
func ginModeForEnv(e Env) string {
	switch e.normalized() {
	case EnvProd, EnvStg, EnvUAT:
		return "release"
	default:
		return "debug"
	}
}

// Load seeds nothing itself: dst MUST be pre-populated (typically with
// DefaultConfig()). It overlays config sources via pkg/config, then Normalize.
func Load(dst *Config, o config.Options) error {
	if dst == nil {
		return errors.New("app: dst must be non-nil")
	}
	if err := config.Load(dst, o); err != nil {
		return err
	}
	dst.Normalize()
	return nil
}

// LoadDefault is a convenience that starts from DefaultConfig(), loads, and
// returns the resolved config.
func LoadDefault(o config.Options) (Config, error) {
	cfg := DefaultConfig()
	if err := Load(&cfg, o); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
