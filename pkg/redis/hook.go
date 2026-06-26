package redis

import (
	"context"
	"strings"
	"time"

	"github.com/redis/rueidis"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// hook implements rueidishook.Hook to add OTel spans and Prometheus metrics
// around every rueidis call. Both collaborators are optional: a nil tracer skips
// spans and a nil metrics set skips metrics, so the hook degrades gracefully and
// is only installed when at least one is active.
//
// Span and metric labels use the command VERB only (e.g. GET), never keys, to
// bound cardinality; multi/pipeline calls collapse to a single "PIPELINE" label.
type hook struct {
	tracer trace.Tracer // nil = no spans
	m      *Metrics     // nil = no metrics
}

const pipelineLabel = "PIPELINE"

// cmdVerb returns the upper-cased command verb (first token) for labelling.
func cmdVerb(commands []string) string {
	if len(commands) == 0 {
		return "unknown"
	}
	return strings.ToUpper(commands[0])
}

// isErr classifies a command error: a redis nil (missing key) is a normal
// outcome, not a failure, so it is not counted as an error.
func isErr(err error) bool { return err != nil && !rueidis.IsRedisNil(err) }

// begin starts a span (when tracing is on) and returns the start time.
func (h *hook) begin(ctx context.Context, verb string) (context.Context, trace.Span, time.Time) {
	start := time.Now()
	if h.tracer == nil {
		return ctx, nil, start
	}
	ctx, span := h.tracer.Start(ctx, "redis."+verb)
	return ctx, span, start
}

// end records duration/status metrics and finishes the span.
func (h *hook) end(span trace.Span, start time.Time, verb string, err error) {
	if h.m != nil {
		status := "ok"
		if isErr(err) {
			status = "error"
		}
		h.m.observe(verb, status, time.Since(start))
	}
	if span != nil {
		if isErr(err) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func (h *hook) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResult {
	verb := cmdVerb(cmd.Commands())
	ctx, span, start := h.begin(ctx, verb)
	resp := client.Do(ctx, cmd)
	h.end(span, start, verb, resp.Error())
	return resp
}

func (h *hook) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) []rueidis.RedisResult {
	ctx, span, start := h.begin(ctx, pipelineLabel)
	resps := client.DoMulti(ctx, multi...)
	h.end(span, start, pipelineLabel, firstErr(resps))
	return resps
}

func (h *hook) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) rueidis.RedisResult {
	verb := cmdVerb(cmd.Commands())
	ctx, span, start := h.begin(ctx, verb)
	resp := client.DoCache(ctx, cmd, ttl)
	if h.m != nil && !isErr(resp.Error()) {
		// Only a completed lookup is a hit or a miss; a transport/error response
		// is neither, so don't let outages masquerade as a flood of cache misses.
		h.m.observeCache(verb, resp.IsCacheHit())
	}
	if span != nil {
		span.SetAttributes(cacheHitAttr(resp.IsCacheHit()))
	}
	h.end(span, start, verb, resp.Error())
	return resp
}

func (h *hook) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) []rueidis.RedisResult {
	ctx, span, start := h.begin(ctx, pipelineLabel)
	resps := client.DoMultiCache(ctx, multi...)
	if h.m != nil {
		for i, resp := range resps {
			if i < len(multi) && !isErr(resp.Error()) {
				h.m.observeCache(cmdVerb(multi[i].Cmd.Commands()), resp.IsCacheHit())
			}
		}
	}
	h.end(span, start, pipelineLabel, firstErr(resps))
	return resps
}

func (h *hook) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) error {
	verb := cmdVerb(subscribe.Commands())
	ctx, span, start := h.begin(ctx, verb)
	err := client.Receive(ctx, subscribe, fn)
	h.end(span, start, verb, err)
	return err
}

func (h *hook) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	verb := cmdVerb(cmd.Commands())
	ctx, span, start := h.begin(ctx, verb)
	stream := client.DoStream(ctx, cmd)
	h.end(span, start, verb, stream.Error())
	return stream
}

func (h *hook) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	ctx, span, start := h.begin(ctx, pipelineLabel)
	stream := client.DoMultiStream(ctx, multi...)
	h.end(span, start, pipelineLabel, stream.Error())
	return stream
}

// firstErr returns the first real (non-nil, non-redis-nil) error in a batch of
// results, used to mark a pipeline's status.
func firstErr(resps []rueidis.RedisResult) error {
	for _, r := range resps {
		if err := r.Error(); isErr(err) {
			return err
		}
	}
	return nil
}

// cacheHitAttr is the span attribute marking a client-side cache hit/miss.
func cacheHitAttr(hit bool) attribute.KeyValue {
	return attribute.Bool("redis.cache_hit", hit)
}
