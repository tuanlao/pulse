package env

import "testing"

func TestValid(t *testing.T) {
	cases := map[string]bool{
		"prod":    true,
		"Dev":     true,
		" UAT ":   true,
		"local":   true,
		"stg":     true,
		"garbage": false,
		"":        false,
	}
	for in, want := range cases {
		if got := Env(in).Valid(); got != want {
			t.Errorf("Env(%q).Valid() = %v, want %v", in, got, want)
		}
	}
}

func TestIsProd(t *testing.T) {
	if !EnvProd.IsProd() {
		t.Fatalf("EnvProd should be prod")
	}
	if EnvDev.IsProd() || EnvStg.IsProd() {
		t.Fatalf("only PROD is prod")
	}
	if !Env("prod").IsProd() {
		t.Fatalf("case-insensitive IsProd failed")
	}
}

func TestNormalize(t *testing.T) {
	cases := map[string]Env{
		"prod":    EnvProd,
		"PROD":    EnvProd,
		" dev ":   EnvDev,
		"uat":     EnvUAT,
		"stg":     EnvStg,
		"local":   EnvLocal,
		"garbage": EnvDev, // unknown -> DEV
		"":        EnvDev,
	}
	for in, want := range cases {
		if got := Env(in).Normalize(); got != want {
			t.Errorf("Env(%q).Normalize() = %q, want %q", in, got, want)
		}
	}
}

func TestGinMode(t *testing.T) {
	cases := map[Env]string{
		EnvProd:        "release",
		EnvStg:         "release",
		EnvUAT:         "release",
		EnvDev:         "debug",
		EnvLocal:       "debug",
		Env("garbage"): "debug", // unknown normalizes to DEV -> debug
	}
	for in, want := range cases {
		if got := in.GinMode(); got != want {
			t.Errorf("Env(%q).GinMode() = %q, want %q", in, got, want)
		}
	}
}
