# `pkg/contextx`

A dependency-free leaf package that carries a request-scoped logger through `context.Context`. It exists primarily to break the import cycle between `pkg/log` and `pkg/tracing`: both packages (and the HTTP middleware) depend only on `contextx`, never on each other. The logger is stored as an `any` so `contextx` itself stays free of any logging dependency; `pkg/log` type-asserts it back to its concrete type.

## Import
```go
import "github.com/tuanlao/pulse/pkg/contextx"
```

## Usage
```go
// Attach a request-scoped logger.
ctx = contextx.WithLogger(ctx, logger)

// Read it back downstream.
if l := contextx.Logger(ctx); l != nil {
	// type-assert to your concrete logger type
}
```

## API / Options
- `WithLogger(ctx context.Context, logger any) context.Context` — returns a copy of `ctx` carrying a request-scoped logger (stored as `any`).
- `Logger(ctx context.Context) any` — returns the stored logger, or `nil` if none was set.

## Notes
- Leaf package with no dependencies beyond the standard library `context` — it is what breaks the `log` ↔ `tracing` import cycle.
- Context keys use an unexported type, preventing collisions with keys defined in other packages.
- The logger is intentionally untyped (`any`); callers (e.g. `pkg/log`) are responsible for type-asserting and supplying a fallback.
