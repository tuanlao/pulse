# pkg/kafka/message

The public, shared envelope types for `pkg/kafka` — a **leaf** package (depends
only on franz-go's `kgo`), so the facade re-exports these without an import cycle.

## Types

- `Message` — the record envelope (Topic/Partition/Offset/Key/Value/Timestamp +
  typed `Headers`). `FromRecord` / `ToRecord` convert to and from `*kgo.Record`;
  `NewMessage(key, value)` stamps a fresh id; `NewID()` returns a uuid.
- `Headers` — wraps a `[]kgo.RecordHeader` slice directly (no map conversion) with
  typed, get/set-only accessors (`MessageID()`, `SetRetryCount()`, …). It holds no
  business logic.
- `Handler` — `func(ctx, *Message) error`.
- `Hooks` — nil-safe, panic-recovered lifecycle observers.
- `ErrorClass` + `NonRetryable(err)` / `IsNonRetryable(err)` — a handler returns
  `NonRetryable(err)` to skip retries and route straight to the DLQ.

## Header keys

`x-message-id`, `x-retry-count`, `x-retry-group`, `x-retry-due-at` (unix millis),
`x-origin-topic` / `-partition` / `-offset` / `-timestamp`, `x-source`,
`x-produced-at`, `x-content-type`, `x-error-class`, `x-error-reason`. Trace context
travels in the standard W3C `traceparent` / `tracestate` (set by `pkg/kafka/trace`).
