# pkg/kafka/consumer

The Kafka **consumer-group** component (`lifecycle.Component`). Built and wired by
the `pkg/kafka` facade — use `kafka.NewConsumer` + `consumer.Register` / `kafka.On`.

## Processing modes (`consumer.mode`)

| Mode | Order | Concurrency | Worker model |
|------|-------|-------------|--------------|
| `unordered` (default) | none | high | `ants` pool fan-out |
| `ordered` | per partition | #partitions | one serial lane per partition |
| `key_ordered` | per key | high | serial lane per key hash, concurrent across keys |

Every mode commits via a **contiguous-offset watermark** (`tracker.go`): only a
gap-free prefix of handled offsets is committed, so out-of-order completion never
commits ahead of an unfinished earlier offset.

## Config (`consumer`)

| Key | Default | Notes |
|-----|---------|-------|
| `group_id` | — | required |
| `topics` | — | origin topics (get the retry/DLQ pipeline) |
| `mode` | `unordered` | `ordered` / `key_ordered` |
| `commit_immediately` | `false` | `true` = at-most-once, high throughput, **accepts message loss** |
| `concurrency` | `256` | ants pool size / lane count |
| `max_poll_records` | `500` | |
| `auto_commit_interval` | `5s` | |
| `drain_timeout` | `30s` | graceful-shutdown drain bound |

## Behavior

- `Register(topic, Handler)` before Start. An origin topic in `Config.Topics`
  with no handler makes Start **fail fast**. Register a handler for
  `retry.DLQTopic(origin)` to consume the DLQ (terminal — no further retry).
- Handler **panics are recovered** into errors (logged with a stack) and routed
  through the normal retry/DLQ pipeline — a poison message never kills a worker or
  the poll loop.
- Retry tiers are consumed in-loop with partition pause/seek-back to honor each
  tier's delay (see `pkg/kafka/retry`).
- `Stop` stops polling, cancels paused timers, drains in-flight handlers (bounded
  by `drain_timeout`; handlers see a cancelled context), commits, then closes.
- The consumer builds its own produce-capable `*kgo.Client` (the group callbacks
  must close over it) and re-uses it for retry/DLQ forwarding.
