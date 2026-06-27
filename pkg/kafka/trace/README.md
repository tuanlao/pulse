# pkg/kafka/trace

Wires Kafka records into OpenTelemetry: it propagates the **W3C trace context**
through record headers (so a trace flows producer → consumer across services) and
starts produce/consume spans.

## Config (`trace`)

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | needs a `TracerProvider` (via `Deps.TracerProvider`) to actually export |

## API

- `Inject(ctx, *kgo.Record)` — write the active span context into the record headers.
- `Extract(ctx, *kgo.Record) context.Context` — read it back; falls back to
  `tracing.WithGeneratedSpanContext` so handler logs always carry a trace id (even
  with export disabled).
- `StartProduceSpan` / `StartConsumeSpan` — producer/consumer-kind spans.
- `Tracer(tp, cfg)` — returns a tracer (no-op when disabled or `tp` is nil).

It hand-rolls a tiny `propagation.TextMapCarrier` over `kgo` record headers and
reuses the same `propagation.TraceContext` the HTTP layer uses — no extra
dependency.
