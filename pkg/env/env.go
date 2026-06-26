// Package env defines the deployment environment enum (LOCAL/DEV/UAT/STG/PROD)
// and the small cross-cutting helpers that used to live on the (now removed)
// unified app config: validation, normalization, and the gin-mode derivation.
//
// Pulse has no god aggregator config: a service owns its own config struct and
// composes the component Configs it needs. This leaf package gives services a
// single, shared definition of Env so the propagation that the old app.Normalize
// did (Env -> tracing.Environment, Env -> gin mode) can be wired explicitly
// without copy-pasting the enum into every service.
package env

import "strings"

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
// Normalize, it does not fall back to a default — an unknown value is invalid.
func (e Env) Valid() bool {
	switch Env(strings.ToUpper(strings.TrimSpace(string(e)))) {
	case EnvLocal, EnvDev, EnvUAT, EnvStg, EnvProd:
		return true
	default:
		return false
	}
}

// IsProd reports whether e is the production environment.
func (e Env) IsProd() bool { return e.Normalize() == EnvProd }

// Normalize upper-cases and trims e; unknown/empty values become EnvDev. It is
// idempotent, so it is safe to call repeatedly.
func (e Env) Normalize() Env {
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

// GinMode maps an Env to a gin mode string ("release" for prod-like
// environments — PROD/STG/UAT — and "debug" otherwise). The return value is a
// plain string (gin's mode vocabulary) so this package stays free of a gin
// dependency; assign it to server.Config.Mode.
func (e Env) GinMode() string {
	switch e.Normalize() {
	case EnvProd, EnvStg, EnvUAT:
		return "release"
	default:
		return "debug"
	}
}
