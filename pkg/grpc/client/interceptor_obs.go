package client

import (
	"context"
	"time"

	"github.com/tuanlao/pulse/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// obsUnary records metrics and a log line for the whole logical call (including
// any gRPC retries below it). It is the outermost interceptor, so latency is
// total wall time and each counter increments once per logical call.
func obsUnary(m *ClientMetrics, logger *log.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		observeCall(ctx, m, logger, cc.Target(), method, "unary", err, time.Since(start))
		return err
	}
}

// obsStream records the stream-establishment latency and a log line. Per-message
// timing is out of scope.
func obsStream(m *ClientMetrics, logger *log.Logger) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		cs, err := streamer(ctx, desc, cc, method, opts...)
		observeCall(ctx, m, logger, cc.Target(), method, streamLabel(desc), err, time.Since(start))
		return cs, err
	}
}

// streamLabel returns the streaming kind of a client stream as a bounded label.
func streamLabel(desc *grpc.StreamDesc) string {
	switch {
	case desc.ClientStreams && desc.ServerStreams:
		return "bidi"
	case desc.ClientStreams:
		return "client_stream"
	default:
		return "server_stream"
	}
}

// observeCall records the metrics and emits the log line shared by the unary and
// stream observability interceptors.
func observeCall(ctx context.Context, m *ClientMetrics, logger *log.Logger, target, method, rpcType string, err error, dur time.Duration) {
	// A done context means the caller canceled or the deadline fired — client-side,
	// not an upstream failure, so keep it out of the warn logs and label it.
	canceled := err != nil && ctx.Err() != nil
	code := status.Code(err)

	if m != nil {
		m.observe(target, method, rpcType, statusLabel(code.String(), canceled), dur)
	}

	if logger != nil {
		l := logger.ForContext(ctx)
		fields := []zap.Field{
			zap.String("method", method),
			zap.String("target", target),
			zap.String("code", code.String()),
			zap.Duration("latency", dur),
		}
		switch {
		case canceled:
			l.Info("outbound grpc canceled", append(fields, zap.Error(err))...)
		case err != nil:
			l.Warn("outbound grpc failed", append(fields, zap.Error(err))...)
		default:
			l.Info("outbound grpc", fields...)
		}
	}
}

// statusLabel maps a code/canceled tuple to a bounded metric label.
func statusLabel(code string, canceled bool) string {
	if canceled {
		return "canceled"
	}
	return code
}
