package interceptor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/tuanlao/pulse/pkg/log"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSplitFullMethod(t *testing.T) {
	cases := []struct {
		in, svc, method string
	}{
		{"/pkg.Svc/Method", "pkg.Svc", "Method"},
		{"/grpc.health.v1.Health/Check", "grpc.health.v1.Health", "Check"},
		{"", "unknown", "unknown"},
		{"/", "unknown", "unknown"},
		{"//", "unknown", "unknown"},
		{"abc", "unknown", "unknown"},
		{"/pkg", "unknown", "unknown"},
		{"/pkg.Svc/", "unknown", "unknown"},
	}
	for _, c := range cases {
		svc, method := splitFullMethod(c.in)
		if svc != c.svc || method != c.method {
			t.Errorf("splitFullMethod(%q) = (%q, %q), want (%q, %q)", c.in, svc, method, c.svc, c.method)
		}
	}
}

func TestRecoveryUnary_PanicToInternal(t *testing.T) {
	logger, read := newCaptureLogger(t)
	resp, err := RecoveryUnary(logger)(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"},
		func(context.Context, any) (any, error) { panic("boom") })

	if resp != nil {
		t.Errorf("resp = %v, want nil", resp)
	}
	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
	if logs := read(); !strings.Contains(logs, "panic recovered") || !strings.Contains(logs, "boom") {
		t.Errorf("logs missing panic info: %s", logs)
	}
}

func TestRecoveryStream_PanicToInternal(t *testing.T) {
	logger, read := newCaptureLogger(t)
	ss := &fakeServerStream{ctx: context.Background()}
	err := RecoveryStream(logger)(nil, ss,
		&grpc.StreamServerInfo{FullMethod: "/pkg.Svc/S"},
		func(any, grpc.ServerStream) error { panic("kaboom") })

	if status.Code(err) != codes.Internal {
		t.Errorf("code = %v, want Internal", status.Code(err))
	}
	if logs := read(); !strings.Contains(logs, "panic recovered") || !strings.Contains(logs, "kaboom") {
		t.Errorf("logs missing panic info: %s", logs)
	}
}

func TestContextLoggerUnary_InjectsLogger(t *testing.T) {
	logger, read := newCaptureLogger(t)
	const traceHex = "0102030405060708090a0b0c0d0e0f10"
	ctx := ctxWithTrace(t, traceHex, "0102030405060708")

	_, err := ContextLoggerUnary(logger)(ctx, nil,
		&grpc.UnaryServerInfo{FullMethod: "/pkg.Svc/M"},
		func(ctx context.Context, _ any) (any, error) {
			if log.FromContext(ctx, nil) == nil {
				t.Error("FromContext returned nil")
			}
			log.FromContext(ctx, logger).Info("handled")
			return "ok", nil
		})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if logs := read(); !strings.Contains(logs, traceHex) {
		t.Errorf("request log missing trace id %s: %s", traceHex, logs)
	}
}

func TestContextLoggerStream_WrapsContext(t *testing.T) {
	logger, read := newCaptureLogger(t)
	const traceHex = "112233445566778899aabbccddeeff00"
	ss := &fakeServerStream{ctx: ctxWithTrace(t, traceHex, "1122334455667788")}

	err := ContextLoggerStream(logger)(nil, ss,
		&grpc.StreamServerInfo{FullMethod: "/pkg.Svc/S"},
		func(_ any, stream grpc.ServerStream) error {
			log.FromContext(stream.Context(), logger).Info("streamed")
			return nil
		})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if logs := read(); !strings.Contains(logs, traceHex) {
		t.Errorf("stream log missing trace id %s: %s", traceHex, logs)
	}
}

// --- test helpers -----------------------------------------------------------

// newCaptureLogger returns a JSON logger writing to a temp file plus a reader
// that syncs and returns the file contents, so tests can assert on log output
// without a zaptest observer (which would require constructing a log.Logger from
// a zap core, not exposed by pkg/log).
func newCaptureLogger(t *testing.T) (*log.Logger, func() string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "log.json")
	logger, err := log.New(log.Config{
		Level:            "debug",
		Encoding:         "json",
		OutputPaths:      []string{path},
		ErrorOutputPaths: []string{path},
	})
	if err != nil {
		t.Fatalf("log.New: %v", err)
	}
	read := func() string {
		_ = logger.Sync()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read log: %v", err)
		}
		return string(b)
	}
	return logger, read
}

func ctxWithTrace(t *testing.T, traceHex, spanHex string) context.Context {
	t.Helper()
	tid, err := trace.TraceIDFromHex(traceHex)
	if err != nil {
		t.Fatalf("trace id: %v", err)
	}
	sid, err := trace.SpanIDFromHex(spanHex)
	if err != nil {
		t.Fatalf("span id: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled})
	return trace.ContextWithSpanContext(context.Background(), sc)
}

// fakeServerStream is a minimal grpc.ServerStream for interceptor tests.
type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context     { return f.ctx }
func (f *fakeServerStream) SendMsg(any) error            { return nil }
func (f *fakeServerStream) RecvMsg(any) error            { return nil }
