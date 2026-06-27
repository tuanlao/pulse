# pkg/kafka/retry

The retry / DLQ engine — the Spring-Kafka non-blocking retry-topics model plus a
blocking in-place model — with responsibilities split for testability:

| Piece | File | Responsibility |
|-------|------|----------------|
| `Classifier` | `classifier.go` | error → `ErrorClass` (empty = retryable) |
| `Planner` | `planner.go` | (message, class) → `Action` (which tier/DLQ, attempt, due time) — pure, no IO |
| `Forwarder` | `forwarder.go` | execute an `Action`: build record + produce + metrics + hooks → `ForwardResult` |
| `DelayScheduler` | `scheduler.go` | enforce a retry record's due time via partition pause + seek-back (message-agnostic) |
| `ReplayPolicy` | `policy.go` | is a DLQ'd class replayable? (policy, overridable) |
| `Namer` | `naming.go` | retry-tier / DLQ topic names |
| `Retrier` | `retrier.go` | thin orchestrator (`Forward`, `RunInPlace`, `EffectiveStrategy`) |

## Config (`retry`)

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `true` | |
| `strategy` | `auto` | `auto`: unordered→non-blocking topics, ordered/key_ordered→blocking; or force `blocking` / `non_blocking` |
| `backoffs` | `[5s,10s,1m]` | tier delays; count = attempt cap |
| `max_attempts` | `len(backoffs)` | clamped to `len(backoffs)` |
| `scope_to_group` | `false` | true → stamp the group so only it reprocesses a retry |
| `retry_suffix_pattern` | `{origin}.retry.{delay}` | tokens `{origin}` `{delay}` `{group}` |
| `dlq_suffix_pattern` | `{origin}.dlq` | |
| `delay_format` | `human` | `5s`/`1m`; or `ms` (`5000`) |
| `max_error_reason_bytes` | `256` | truncates the `x-error-reason` header |
| `dlq.enabled` | `true` | exhausted/non-retryable → DLQ; false = drop |

## Model

- **Non-blocking** (unordered): a failed message is forwarded to the next delay
  tier (`origin.retry.5s` → `.10s` → `.1m`), exhausting into `origin.dlq`. Each tier
  topic has one fixed delay, so the consumer pauses the partition on the head
  record until its due time (background heartbeats keep the group membership).
  A message is "handled" — the origin offset advances — once it is durably
  produced to the next topic.
- **Blocking** (ordered/key_ordered): the handler is retried in place
  (order-preserving) up to `blocking_max_attempts`, then routed to the DLQ.
- `NonRetryable(err)` (from `pkg/kafka/message`) skips retries → DLQ with class
  `non_retryable`. `Replayable(class)` decides replayability — a policy, not a
  header property.
