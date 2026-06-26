// Package contextx is a dependency-free leaf package that carries a
// request-scoped logger through context.Context.
//
// It exists primarily to break the import cycle between pkg/log and
// pkg/tracing: both packages (and the HTTP middleware) depend only on contextx,
// never on each other. The request-scoped logger is stored as an `any` so that
// contextx itself stays free of any logging dependency; pkg/log type-asserts it
// back to its concrete type.
package contextx

import "context"

// ctxKey is an unexported type for context keys defined in this package. Using
// an unexported type prevents collisions with keys defined in other packages.
type ctxKey int

const (
	loggerKey ctxKey = iota
)

// WithLogger returns a copy of ctx carrying a request-scoped logger. The logger
// is stored as an `any` so this package has no dependency on a logging library;
// pkg/log provides typed helpers around it.
func WithLogger(ctx context.Context, logger any) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// Logger returns the request-scoped logger previously stored with WithLogger,
// or nil if none was set. Callers (pkg/log) are responsible for type-asserting
// and supplying a fallback.
func Logger(ctx context.Context) any {
	return ctx.Value(loggerKey)
}
