// Package log provides a zap-based structured logger with a console format of
// "[time][LEVEL] message field=value ...". It is context-aware: a request-scoped
// logger (carrying trace id / span id) is stored in and retrieved
// from context via pkg/contextx, and the trace/span ids are read from the OTel
// trace API (not the SDK), so this package never imports pkg/tracing.
package log

import (
	"context"
	"strings"
	"time"

	"github.com/tuanlao/pulse/pkg/contextx"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config configures the logger. All fields are overridable.
type Config struct {
	// Level is the minimum enabled level: debug, info, warn, error, dpanic,
	// panic, fatal. Default "info".
	Level string `mapstructure:"level"`
	// Encoding is "console" (the bracketed human format) or "json". Default
	// "console".
	Encoding string `mapstructure:"encoding"`
	// Development toggles development-friendly settings (stacktraces on warn+).
	Development bool `mapstructure:"development"`
	// OutputPaths are zap sinks. Default ["stderr"].
	OutputPaths []string `mapstructure:"output_paths"`
	// ErrorOutputPaths are zap internal-error sinks. Default ["stderr"].
	ErrorOutputPaths []string `mapstructure:"error_output_paths"`

	// TraceField is the log key for the OTel trace id. Default "trace_id".
	TraceField string `mapstructure:"trace_field"`
	// SpanField is the log key for the OTel span id. Default "span_id".
	SpanField string `mapstructure:"span_field"`
}

// DefaultConfig returns Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Level:            "info",
		Encoding:         "console",
		Development:      false,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
		TraceField:       "trace_id",
		SpanField:        "span_id",
	}
}

func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.Level == "" {
		c.Level = d.Level
	}
	if c.Encoding == "" {
		c.Encoding = d.Encoding
	}
	if len(c.OutputPaths) == 0 {
		c.OutputPaths = d.OutputPaths
	}
	if len(c.ErrorOutputPaths) == 0 {
		c.ErrorOutputPaths = d.ErrorOutputPaths
	}
	if c.TraceField == "" {
		c.TraceField = d.TraceField
	}
	if c.SpanField == "" {
		c.SpanField = d.SpanField
	}
}

// Option overrides Config fields.
type Option func(*Config)

// WithLevel sets the minimum log level.
func WithLevel(level string) Option { return func(c *Config) { c.Level = level } }

// WithEncoding sets the encoding ("console" or "json").
func WithEncoding(enc string) Option { return func(c *Config) { c.Encoding = enc } }

// WithFieldNames overrides the trace/span field keys. Empty strings keep the
// existing value.
func WithFieldNames(trace, span string) Option {
	return func(c *Config) {
		if trace != "" {
			c.TraceField = trace
		}
		if span != "" {
			c.SpanField = span
		}
	}
}

// Logger wraps *zap.Logger and remembers the configured field names so context
// extraction stays consistent.
type Logger struct {
	z   *zap.Logger
	cfg Config
}

// New builds a Logger from cfg + opts.
func New(cfg Config, opts ...Option) (*Logger, error) {
	cfg.applyDefaults()
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg.applyDefaults() // re-apply in case an option cleared a field

	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(strings.ToLower(cfg.Level))); err != nil {
		return nil, err
	}

	zc := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      cfg.Development,
		Encoding:         cfg.Encoding,
		EncoderConfig:    encoderConfig(cfg.Encoding),
		OutputPaths:      cfg.OutputPaths,
		ErrorOutputPaths: cfg.ErrorOutputPaths,
	}

	// Skip one frame so the caller reported in logs is the user's call site, not
	// this package's wrapper methods (Info/Warn/Error/Debug, ForContext, the
	// lifecycle adapter — all add exactly one frame).
	z, err := zc.Build(zap.AddCallerSkip(1))
	if err != nil {
		return nil, err
	}
	return &Logger{z: z, cfg: cfg}, nil
}

// encoderConfig builds the zap EncoderConfig. For "console" it produces the
// "[time][LEVEL]" bracketed prefix; for "json" it uses conventional keys.
func encoderConfig(encoding string) zapcore.EncoderConfig {
	ec := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	if encoding == "console" {
		// Wrap time and level in brackets: "[2026-06-25T...][INFO]".
		ec.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString("[" + t.Format("2006-01-02T15:04:05.000Z0700") + "]")
		}
		ec.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString("[" + l.CapitalString() + "]")
		}
		ec.ConsoleSeparator = " "
	}
	return ec
}

// Zap returns the underlying *zap.Logger for advanced use.
func (l *Logger) Zap() *zap.Logger { return l.z }

// Config returns the resolved configuration (with field names).
func (l *Logger) Config() Config { return l.cfg }

// With returns a child Logger with the given fields permanently attached.
func (l *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{z: l.z.With(fields...), cfg: l.cfg}
}

// Sync flushes buffered log entries. It ignores the harmless "invalid argument"
// error that Sync returns for console/stderr sinks on some platforms.
func (l *Logger) Sync() error {
	err := l.z.Sync()
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "invalid argument") ||
		strings.Contains(msg, "inappropriate ioctl for device") ||
		strings.Contains(msg, "bad file descriptor") {
		return nil
	}
	return err
}

// Convenience leveled methods on the base logger.
func (l *Logger) Debug(msg string, f ...zap.Field) { l.z.Debug(msg, f...) }
func (l *Logger) Info(msg string, f ...zap.Field)  { l.z.Info(msg, f...) }
func (l *Logger) Warn(msg string, f ...zap.Field)  { l.z.Warn(msg, f...) }
func (l *Logger) Error(msg string, f ...zap.Field) { l.z.Error(msg, f...) }

// ForContext builds a request-scoped Logger by attaching the OTel trace/span
// ids (when present) as fields, using this logger's configured field names. If
// ctx carries no valid span context it returns the receiver unchanged.
func (l *Logger) ForContext(ctx context.Context) *Logger {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return l
	}
	return l.With(
		zap.String(l.cfg.TraceField, sc.TraceID().String()),
		zap.String(l.cfg.SpanField, sc.SpanID().String()),
	)
}

// IntoContext stores the given request-scoped logger in ctx for later retrieval
// by FromContext.
func IntoContext(ctx context.Context, l *Logger) context.Context {
	return contextx.WithLogger(ctx, l)
}

// FromContext returns the request-scoped Logger previously stored with
// IntoContext, or fallback if none is present. If fallback is nil, a no-op
// logger is returned so callers never get nil.
func FromContext(ctx context.Context, fallback *Logger) *Logger {
	if v := contextx.Logger(ctx); v != nil {
		if l, ok := v.(*Logger); ok {
			return l
		}
	}
	if fallback != nil {
		return fallback
	}
	return Nop()
}

// Nop returns a Logger that discards everything. Useful as a safe default.
func Nop() *Logger {
	return &Logger{z: zap.NewNop(), cfg: DefaultConfig()}
}

// LifecycleAdapter adapts a Logger to the lifecycle.Logger interface
// (Info/Error with key-value variadics).
func (l *Logger) LifecycleAdapter() *kvAdapter { return &kvAdapter{l: l} }

// GocronAdapter adapts a Logger to gocron's Logger interface (Debug/Info/Warn/
// Error with key-value variadics). It must be defined here, not in pkg/cron, so
// it can log through the underlying zap logger directly (a.l.z) rather than the
// public wrapper methods: New bakes in a single AddCallerSkip(1) that assumes
// exactly one wrapper frame, so going through a wrapper would misattribute the
// caller to the adapter instead of the real call site.
func (l *Logger) GocronAdapter() *kvAdapter { return &kvAdapter{l: l} }

// kvAdapter bridges *Logger to foreign slog-style key/value logger interfaces
// (lifecycle.Logger, gocron.Logger). It calls a.l.z.* directly so exactly one
// wrapper frame sits between the caller and zap, matching New's AddCallerSkip(1).
type kvAdapter struct{ l *Logger }

func (a *kvAdapter) Debug(msg string, kv ...any) { a.l.z.Debug(msg, SugarFields(kv)...) }
func (a *kvAdapter) Info(msg string, kv ...any)  { a.l.z.Info(msg, SugarFields(kv)...) }
func (a *kvAdapter) Warn(msg string, kv ...any)  { a.l.z.Warn(msg, SugarFields(kv)...) }
func (a *kvAdapter) Error(msg string, kv ...any) { a.l.z.Error(msg, SugarFields(kv)...) }

// SugarFields converts a flat slog-style key/value list into zap fields,
// tolerating odd lengths and non-string keys. error values are attached as
// zap.NamedError. It is the shared helper for adapting *log.Logger to foreign
// key/value logger interfaces (e.g. lifecycle.Logger, gocron.Logger).
func SugarFields(kv []any) []zap.Field {
	fields := make([]zap.Field, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			key = "field"
		}
		if err, ok := kv[i+1].(error); ok {
			fields = append(fields, zap.NamedError(key, err))
			continue
		}
		fields = append(fields, zap.Any(key, kv[i+1]))
	}
	return fields
}
