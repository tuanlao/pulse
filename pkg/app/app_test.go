package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tuanlao/pulse/pkg/config"
)

func TestEnv(t *testing.T) {
	if !EnvProd.IsProd() || EnvDev.IsProd() {
		t.Fatalf("IsProd wrong")
	}
	if !Env("prod").Valid() || !Env("Dev").Valid() {
		t.Fatalf("case-insensitive Valid failed")
	}
	if Env("garbage").Valid() {
		t.Fatalf("garbage should be invalid")
	}
	if Env("garbage").normalized() != EnvDev {
		t.Fatalf("unknown env should normalize to DEV")
	}
}

func TestDefaultConfig(t *testing.T) {
	c := DefaultConfig()
	if c.Env != EnvDev {
		t.Fatalf("default env should be DEV")
	}
	if c.Server.Mode != "" {
		t.Fatalf("Server.Mode should be empty in defaults so Env can drive it")
	}
	if c.HttpClients == nil {
		t.Fatalf("HttpClients map should be initialized")
	}
}

func TestNormalize_PropagatesAndDerives(t *testing.T) {
	c := DefaultConfig()
	c.Env = "prod"
	c.ServiceName = "billing"
	c.Version = "1.2.3"
	c.Normalize()

	if c.Env != EnvProd {
		t.Fatalf("env not normalized: %s", c.Env)
	}
	if c.Tracing.ServiceName != "billing" {
		t.Fatalf("service name not propagated to tracing")
	}
	if c.Tracing.ServiceVersion != "1.2.3" {
		t.Fatalf("version not propagated to tracing")
	}
	if c.Tracing.Environment != "PROD" {
		t.Fatalf("env not propagated to tracing.Environment: %s", c.Tracing.Environment)
	}
	if c.Server.Mode != "release" {
		t.Fatalf("prod should derive gin release mode, got %s", c.Server.Mode)
	}
}

func TestNormalize_DevGinMode(t *testing.T) {
	c := DefaultConfig()
	c.Env = EnvDev
	c.Normalize()
	if c.Server.Mode != "debug" {
		t.Fatalf("dev should derive gin debug mode, got %s", c.Server.Mode)
	}
}

func TestNormalize_RespectsExplicitMode(t *testing.T) {
	c := DefaultConfig()
	c.Env = EnvDev
	c.Server.Mode = "release" // explicit override
	c.Normalize()
	if c.Server.Mode != "release" {
		t.Fatalf("explicit mode must be respected, got %s", c.Server.Mode)
	}
}

func TestLoad_ObjectYAMLWithHttpClients(t *testing.T) {
	dir := t.TempDir()
	yaml := `
env: UAT
service_name: gateway
server:
  port: 9999
http_clients:
  payment:
    base_url: https://payment.internal
    pool:
      max_idle_conns_per_host: 50
  user:
    base_url: https://user.internal
    timeouts:
      request: 5s
`
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(yaml), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg := DefaultConfig()
	if err := Load(&cfg, config.Options{SearchPaths: []string{dir}}); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Env != EnvUAT {
		t.Fatalf("env = %s, want UAT", cfg.Env)
	}
	if cfg.Server.Port != 9999 {
		t.Fatalf("server port not loaded: %d", cfg.Server.Port)
	}
	if cfg.Server.Mode != "release" { // UAT -> release
		t.Fatalf("UAT should derive release, got %s", cfg.Server.Mode)
	}
	pay, ok := cfg.HttpClients["payment"]
	if !ok || pay.BaseURL != "https://payment.internal" {
		t.Fatalf("payment client not loaded: %+v", cfg.HttpClients)
	}
	if pay.Pool.MaxIdleConnsPerHost != 50 {
		t.Fatalf("nested pool object not loaded: %d", pay.Pool.MaxIdleConnsPerHost)
	}
	usr := cfg.HttpClients["user"]
	if usr.Timeouts.Request.String() != "5s" {
		t.Fatalf("nested duration in map not decoded: %s", usr.Timeouts.Request)
	}
	// ServiceName propagated.
	if cfg.Tracing.ServiceName != "gateway" {
		t.Fatalf("service name not propagated after Load")
	}
}
