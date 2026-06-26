package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

type corsCfg struct {
	AllowOrigins []string `mapstructure:"allow_origins"`
}

type httpCfg struct {
	Addr        string        `mapstructure:"addr"`
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
	CORS        corsCfg       `mapstructure:"cors"`
}

type appCfg struct {
	HTTP httpCfg `mapstructure:"http"`
}

func defaultAppCfg() appCfg {
	return appCfg{
		HTTP: httpCfg{
			Addr:        ":8080",
			ReadTimeout: 15 * time.Second,
			CORS:        corsCfg{AllowOrigins: []string{"*"}},
		},
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return dir
}

func TestLoad_DefaultsOnlyWhenNoFile(t *testing.T) {
	cfg := defaultAppCfg()
	// Point at an empty temp dir so no file is found.
	if err := Load(&cfg, Options{SearchPaths: []string{t.TempDir()}}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" || cfg.HTTP.ReadTimeout != 15*time.Second {
		t.Fatalf("defaults not preserved: %+v", cfg.HTTP)
	}
	if len(cfg.HTTP.CORS.AllowOrigins) != 1 || cfg.HTTP.CORS.AllowOrigins[0] != "*" {
		t.Fatalf("nested default not preserved: %+v", cfg.HTTP.CORS)
	}
}

func TestLoad_FileOverridesDefault(t *testing.T) {
	dir := writeConfig(t, "http:\n  addr: \":9090\"\n  read_timeout: 30s\n")
	cfg := defaultAppCfg()
	if err := Load(&cfg, Options{SearchPaths: []string{dir}}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":9090" {
		t.Fatalf("addr = %q, want :9090", cfg.HTTP.Addr)
	}
	if cfg.HTTP.ReadTimeout != 30*time.Second {
		t.Fatalf("read_timeout = %v, want 30s (duration hook)", cfg.HTTP.ReadTimeout)
	}
	// Untouched key keeps default.
	if cfg.HTTP.CORS.AllowOrigins[0] != "*" {
		t.Fatalf("untouched nested key changed: %+v", cfg.HTTP.CORS)
	}
}

func TestLoad_NestedObjectAndSlice(t *testing.T) {
	dir := writeConfig(t, "http:\n  cors:\n    allow_origins:\n      - https://a.com\n      - https://b.com\n")
	cfg := defaultAppCfg()
	if err := Load(&cfg, Options{SearchPaths: []string{dir}}); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.HTTP.CORS.AllowOrigins) != 2 || cfg.HTTP.CORS.AllowOrigins[0] != "https://a.com" {
		t.Fatalf("nested object/slice not loaded: %+v", cfg.HTTP.CORS)
	}
}

func TestLoad_NilDst(t *testing.T) {
	var p *appCfg
	if err := Load(p, DefaultOptions()); err == nil {
		t.Fatalf("expected error for nil dst")
	}
}
