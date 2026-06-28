// Package interceptor provides the gRPC server interceptors pulse's gRPC server
// chains together: panic recovery (converting a panic into a codes.Internal
// status) and request-scoped logging carrying the OTel trace/span ids. RED
// metrics live in metrics.go in this package; OTel span instrumentation comes
// from otelgrpc's StatsHandler, wired by pkg/grpc/server.
//
// The chain mirrors the HTTP server's middleware order (recovery → context
// logger → metrics): recovery is outermost so it catches panics from the logger,
// metrics and handler; the context logger runs after the otelgrpc StatsHandler
// (which brackets the whole RPC) so the trace/span ids are already on the context.
package interceptor

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/tuanlao/pulse/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// splitFullMethod splits a gRPC full method ("/pkg.Service/Method") into its
// service and method parts. Both come from the static proto definition, never
// from user data, so they are safe (bounded-cardinality) metric labels. Any
// malformed input ("", "/", "//", "abc", "/pkg") collapses to
// ("unknown", "unknown") so a stray value cannot explode label cardinality.
func splitFullMethod(full string) (service, method string) {
	full = strings.TrimPrefix(full, "/")
	i := strings.LastIndex(full, "/")
	if i <= 0 || i >= len(full)-1 {
		return "unknown", "unknown"
	}
	return full[:i], full[i+1:]
}

// streamType returns the streaming kind of a server stream as a bounded label.
// A server stream is at least server-streaming if it is not client-streaming.
func streamType(info *grpc.StreamServerInfo) string {
	switch {
	case info.IsClientStream && info.IsServerStream:
		return "bidi"
	case info.IsClientStream:
		return "client_stream"
	default:
		return "server_stream"
	}
}

// RecoveryUnary returns a unary server interceptor that recovers panics from the
// handler, logs them (with the request-scoped logger when present, falling back
// to base) and converts them into a codes.Internal status so a single panicking
// handler cannot crash the server. It is the outermost interceptor in the chain.
func RecoveryUnary(base *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				logPanic(ctx, base, info.FullMethod, rec)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

// RecoveryStream is the streaming counterpart of RecoveryUnary.
func RecoveryStream(base *log.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				logPanic(ss.Context(), base, info.FullMethod, rec)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

// logPanic logs a recovered panic. The value is rendered with %+v so types
// implementing fmt.Formatter keep their contextual output (a plain zap.Any would
// lose it).
func logPanic(ctx context.Context, base *log.Logger, fullMethod string, rec any) {
	log.FromContext(ctx, base).Error("panic recovered",
		zap.String("method", fullMethod),
		zap.String("panic", fmt.Sprintf("%+v", rec)),
		zap.ByteString("stack", debug.Stack()),
	)
}

// ContextLoggerUnary returns a unary server interceptor that derives a
// request-scoped logger (carrying the OTel trace/span ids established by the
// otelgrpc StatsHandler) and stores it in the context so handlers can retrieve it
// via log.FromContext.
func ContextLoggerUnary(base *log.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx = log.IntoContext(ctx, base.ForContext(ctx))
		return handler(ctx, req)
	}
}

// ContextLoggerStream is the streaming counterpart of ContextLoggerUnary. It
// wraps the ServerStream so the handler's ss.Context() returns the
// logger-carrying context.
func ContextLoggerStream(base *log.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := log.IntoContext(ss.Context(), base.ForContext(ss.Context()))
		return handler(srv, &loggedStream{ServerStream: ss, ctx: ctx})
	}
}

// loggedStream overrides Context() so a server-stream handler sees the
// logger-carrying context.
type loggedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *loggedStream) Context() context.Context { return s.ctx }
