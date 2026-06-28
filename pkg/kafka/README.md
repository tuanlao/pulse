# pkg/kafka

A composable Kafka component for pulse, built on [franz-go](https://github.com/twmb/franz-go).
It provides a producer and a consumer-group consumer with **retry topics + DLQ**
(Spring-Kafka style), **deduplication**, three **processing modes**, two **commit
modes**, **auto topic provisioning**, full **headers**, **lifecycle hooks**,
**metrics** and **tracing** — every bit config-driven.

To stay maintainable, the component is split into focused sub-packages behind a
thin facade. Callers import only `pkg/kafka`.

```
pkg/kafka/            facade: Config, Deps, NewProducer, NewConsumer, NewConsumers/ConsumerSet, Send[T], On[T], aliases
  message/            Message envelope, typed Headers, Hooks, ErrorClass, NonRetryable
  codec/              Codec interface + JSON default (swap in protobuf/msgpack via Deps.Codec)
  producer/           Producer (lifecycle.Component): Produce / ProduceSync / Send
  consumer/           Consumer (lifecycle.Component): modes, watermark tracker, poll loop
  retry/              Classifier -> Planner -> Forwarder, DelayScheduler, ReplayPolicy, naming
  dedup/              Deduper: local (otter) | redis (rueidis, optional client-side cache)
  admin/              topic provisioning: EnsureTopics / ValidateTopics
  metrics/            package-owned Prometheus collectors
  trace/              W3C trace propagation over record headers + spans
  internal/kclient/   connection config (brokers/sasl/tls) + client builder
```

## Quick start

```go
m, _ := kafka.NewMetrics(cfg.Kafka.Metrics, red.Registry())
deps := kafka.Deps{Logger: logger, TracerProvider: tp, Metrics: m}
// global dedup: deps.RedisClient = rdb

producer, _ := kafka.NewProducer(cfg.Kafka, deps)
consumer, _ := kafka.NewConsumer(cfg.Kafka, deps)

// Typed handler — payload decoded into OrderCreated before it runs.
kafka.On(consumer, "orders", func(ctx context.Context, e OrderCreated, m *kafka.Message) error {
    if !e.Valid() {
        return kafka.NonRetryable(errors.New("invalid order")) // -> DLQ, no retries
    }
    return process(ctx, e) // a returned error -> retry tiers
})
// consume the DLQ too (optional) — it is just another topic:
kafka.On(consumer, kafka.DLQTopic(cfg.Kafka, "orders"), handleDLQ)

mgr.Register(producer, consumer) // implement lifecycle.Component

// produce:
_ = kafka.Send(ctx, producer, "orders", order.ID, OrderCreated{...})
```

## Multiple consumers / multiple groups (`consumers`)

`NewConsumer` builds one consumer from `consumer` + `retry` + `dedup`. To consume
different topics under different groups from one service, declare a `consumers` map
— each entry is **fully independent** (its own `group_id`, `topics`, `mode`,
`concurrency`, `retry`/DLQ and `dedup`) — and build them with `NewConsumers`:

```yaml
kafka:
  client: { brokers: ["localhost:9092"] }   # shared: one cluster per service
  consumers:
    orders:                                  # <- the name you Get() in code
      group_id: svc-orders
      topics: ["orders"]
      mode: unordered
      retry: { enabled: true, backoffs: ["5s","1m"], dlq: { enabled: true } }
    audit:                                   # different group => its own copy of each record
      group_id: svc-audit
      topics: ["orders", "users"]
      mode: ordered
      retry: { enabled: false }
```

```go
consumers, _ := kafka.NewConsumers(cfg.Kafka, deps)

if c, ok := consumers.Get("orders"); ok {   // look up by the config name
    kafka.On(c, "orders", handleOrder)
}
if c, ok := consumers.Get("audit"); ok {
    kafka.On(c, "orders", handleAudit)
    kafka.On(c, "users", handleUser)
}

mgr.Register(consumers.Components()...)      // register them all (each its own group)
```

`ConsumerSet` also has `MustGet`, `Names`, `Each`, `Len`. The shared connection,
admin (topic provisioning), tracing and metrics come from the parent `Config`.

**Manual / no config** — build one programmatically instead (e.g. for a one-off):

```go
c, _ := kafka.NewConsumer(cfg.Kafka, deps,
    kafka.WithGroupID("ad-hoc"), kafka.WithTopics("ad-hoc-topic"))
// or start from kafka.DefaultConsumerConfig() and set fields yourself.
```

## Processing modes (`consumer.mode`)

| Mode | Order guarantee | Concurrency | Worker model |
|------|-----------------|-------------|--------------|
| `unordered` (default) | none | high | `ants` pool fan-out |
| `ordered` | per partition | #partitions | one serial lane per partition |
| `key_ordered` | per message key | high | serial lane per key hash, concurrent across keys |

In every mode, **offset commits use a contiguous watermark**: only a gap-free
prefix of handled offsets is committed, so out-of-order completion never commits
ahead of an unfinished earlier offset (which would drop it on crash).

## Retry & DLQ (Spring-Kafka non-blocking retry topics)

`strategy: auto` picks the model per mode:

- **`unordered` → non-blocking retry topics.** A failed message is forwarded to a
  per-delay retry topic — `orders.retry.5s` → `orders.retry.10s` → `orders.retry.1m`
  (from `retry.backoffs`) — and, once the tiers are exhausted, to `orders.dlq`.
  The delay is enforced by **pausing the partition and seeking back** until the
  record's due time (no busy-loop; franz-go heartbeats in the background, so a
  paused partition keeps its group membership). A message is considered handled —
  and its origin offset advances — as soon as it is **durably produced** to the
  next topic.
- **`ordered` / `key_ordered` → blocking in-place retry.** The handler is retried
  in place (preserving order) after each backoff, then routed to the DLQ. (Forwarding
  to a retry topic would reorder, so it is not used here.)

Override with `strategy: blocking | non_blocking`.

Retry/DLQ records carry `x-retry-count`, `x-retry-due-at`, the origin coordinates,
and `x-error-class` / `x-error-reason` (the reason is truncated to
`max_error_reason_bytes`, default 256 — full detail goes to the log). By default a
retry is **not** scoped to a group (`x-retry-group` empty → any group reprocesses);
set `retry.scope_to_group: true` to restrict it to the producing group, in which
case other groups skip it.

Error classification: a handler returns `kafka.NonRetryable(err)` to skip retries
and go straight to the DLQ with class `non_retryable`. Whether a DLQ'd class is
replayable is a **policy** (`retry.ReplayPolicy` / `kafka.Replayable`), not a header
concern.

## Commit modes (`consumer.commit_immediately`)

- `false` (default) — **watermark / at-least-once.** Safe; no message loss.
- `true` — **immediate / at-most-once.** Offsets commit as records are polled
  (no tracker), maximizing throughput but **accepting message loss** on crash.
  Pair with `mode: unordered`.

## Deduplication (`dedup`, opt-in)

Checks `x-message-id` before processing and marks it only after success, so a
redelivered duplicate is skipped while a failed message is still retried.
`mode: local` uses an in-process [otter](https://github.com/maypok86/otter) cache
(per pod); `mode: redis` shares state across pods via rueidis, optionally served
from client-side cache (`redis.client_side_cache`, BCAST). Recommended for
`unordered` + retry. The `ttl` must exceed the total retry lifetime. Dedup is
best-effort de-amplification, not exactly-once.

## Topic provisioning (`admin`)

`auto_create: true` (default) idempotently creates origin, retry-tier and DLQ
topics with `partitions` (default 4) and `replication_factor` (default 1, set ≥3
in prod). `auto_create: false` instead **validates** they exist on Start and fails
fast — the enterprise pattern where topics are managed out of band.

## Config

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | when false, New* return no-op components |
| `client.brokers` | `["localhost:9092"]` | seed brokers |
| `client.client_id` | `pulse` | |
| `client.sasl` | disabled | `plain` / `scram-sha-256` / `scram-sha-512` |
| `client.tls` | disabled | CA / mTLS / `insecure_skip_verify` |
| `producer.required_acks` | `all` | `leader` / `none` disable idempotency |
| `producer.linger` | `10ms` | |
| `producer.compression` | `snappy` | `gzip`/`lz4`/`zstd`/`none` |
| `consumer.group_id` | — | required to consume (single/manual consumer) |
| `consumer.topics` | — | origin topics (get the retry/DLQ pipeline) |
| `consumer.mode` | `unordered` | `ordered` / `key_ordered` |
| `consumer.commit_immediately` | `false` | `true` = at-most-once |
| `consumer.concurrency` | `256` | ants pool size / lane count |
| `consumer.max_poll_records` | `500` | |
| `consumer.drain_timeout` | `30s` | graceful-shutdown drain bound |
| `consumers.<name>` | — | a named independent consumer; same fields as `consumer.*` **plus** its own nested `retry` / `dedup`. Built by `NewConsumers`, looked up by `<name>` |
| `admin.auto_create` | `true` | else validate + fail fast |
| `admin.partitions` | `4` | |
| `admin.replication_factor` | `1` | ≥3 in prod |
| `retry.enabled` | `true` | |
| `retry.strategy` | `auto` | `blocking` / `non_blocking` |
| `retry.backoffs` | `[5s,10s,1m]` | tier delays (count = attempt cap) |
| `retry.scope_to_group` | `false` | true → only the producing group reprocesses |
| `retry.max_error_reason_bytes` | `256` | truncates `x-error-reason` |
| `retry.dlq.enabled` | `true` | |
| `dedup.enabled` | `false` | opt-in |
| `dedup.mode` | `local` | `redis` (needs `Deps.RedisClient`) |
| `dedup.ttl` | `1h` | must exceed total retry lifetime |
| `metrics.enabled` | `true` | namespace `pulse`, subsystem `kafka` |
| `trace.enabled` | `true` | needs `Deps.TracerProvider` to export |

## Headers

`x-message-id`, `x-retry-count`, `x-retry-group`, `x-retry-due-at`,
`x-origin-topic` / `-partition` / `-offset` / `-timestamp`, `x-source`,
`x-produced-at`, `x-content-type`, `x-error-class`, `x-error-reason`. Trace context
travels in the standard W3C `traceparent` / `tracestate`. Access them via the typed
`message.Headers` helper (`MessageID()`, `SetRetryCount()`, …) rather than string
keys.

## Metrics

`pulse_kafka_produce_total`, `_produce_duration_seconds`, `_consume_total`
(status: success/error/retry/dlq/dedup_skip/group_skip), `_consume_duration_seconds`,
`_retries_total`, `_dlq_total{class}`, `_dedup_skipped_total`, `_group_skipped_total`,
`_backoff_paused` (gauge), `_handler_in_flight` (gauge). Labels are bounded to
topic / group / status / class (never keys or partitions). Build with
`kafka.NewMetrics(cfg, registry)` sharing the server's registry.

## Hooks

`Deps.Hooks` (all nil-safe, panic-recovered) observe the lifecycle: `OnProduce`,
`OnProduceError`, `OnConsume`, `OnSuccess`, `OnError`, `OnRetry`, `OnDLQ`,
`OnDedupeSkip`, `OnGroupSkip`, `OnBackoffPause`.

## Notes

- **Graceful shutdown.** `Stop` stops polling, cancels paused retry timers, drains
  in-flight handlers (bounded by `drain_timeout`, handlers see a cancelled context),
  commits the watermark, then closes the client.
- **Rebalance.** In watermark mode rebalances are blocked during a poll; on revoke
  the marked (handled) offsets are committed. In-flight records not yet handled are
  reprocessed by the new owner (at-least-once) — enable dedup to absorb that.
- **Delayed retry is via dedicated tiers**, configured by `retry.backoffs`. There
  is no separate "scheduled message" facility beyond these tiers.
- The whole library is unified on franz-go; topic admin uses `kadm`.
