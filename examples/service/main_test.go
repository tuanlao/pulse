package main

import (
	"testing"

	"github.com/tuanlao/pulse/pkg/env"
)

// These tests cover the cross-cutting propagation that normalize() took over from
// the removed app.Normalize (Env normalization, ServiceName/Version -> tracing,
// Env -> tracing.Environment, Env -> gin Server.Mode). They moved here when
// pkg/app/app_test.go was deleted so the wiring keeps a regression guard.

func TestNormalize_PropagatesAndDerives(t *testing.T) {
	c := defaultAppConfig()
	c.Env = "prod"
	c.ServiceName = "billing"
	c.Version = "1.2.3"
	c.normalize()

	if c.Env != env.EnvProd {
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
	c := defaultAppConfig()
	c.Env = env.EnvDev
	c.normalize()
	if c.Server.Mode != "debug" {
		t.Fatalf("dev should derive gin debug mode, got %s", c.Server.Mode)
	}
}

func TestNormalize_RespectsExplicitMode(t *testing.T) {
	c := defaultAppConfig()
	c.Env = env.EnvDev
	c.Server.Mode = "release" // explicit override must survive
	c.normalize()
	if c.Server.Mode != "release" {
		t.Fatalf("explicit mode must be respected, got %s", c.Server.Mode)
	}
}

func TestNormalize_DoesNotOverrideExplicitTracing(t *testing.T) {
	c := defaultAppConfig()
	c.ServiceName = "billing"
	c.Tracing.ServiceName = "explicit" // already set -> must not be clobbered
	c.normalize()
	if c.Tracing.ServiceName != "explicit" {
		t.Fatalf("explicit tracing service name must be respected, got %s", c.Tracing.ServiceName)
	}
}

func TestNormalize_Idempotent(t *testing.T) {
	c := defaultAppConfig()
	c.Env = "prod"
	c.ServiceName = "billing"
	c.normalize()
	first := c
	c.normalize() // calling again changes nothing
	if c.Env != first.Env || c.Tracing.ServiceName != first.Tracing.ServiceName ||
		c.Tracing.Environment != first.Tracing.Environment || c.Server.Mode != first.Server.Mode {
		t.Fatalf("normalize is not idempotent: %+v vs %+v", first, c)
	}
}
