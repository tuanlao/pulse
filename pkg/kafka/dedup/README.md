# pkg/kafka/dedup

Best-effort message **deduplication** for the consumer. The `x-message-id` header
is checked before processing and marked only **after a successful handle**, so a
redelivered duplicate is skipped while a failed message is still retried.

## Config (`dedup`)

| Key | Default | Notes |
|-----|---------|-------|
| `enabled` | `false` | opt-in |
| `mode` | `local` | `local` (otter, per-pod) or `redis` (shared across pods) |
| `ttl` | `1h` | **must exceed the total retry lifetime** |
| `local.max_size` | `100000` | otter cache cap |
| `redis.key_prefix` | `pulse:kafka:dedup` | |
| `redis.client_side_cache` | `false` | serve the existence check from rueidis client-side cache (DoCache) |
| `redis.cache_ttl` | `30s` | client-side cache entry lifetime |

## API

`Deduper` interface: `Seen(ctx, id) (bool, error)`, `Mark(ctx, id, ttl) error`.
`New(cfg, redisClient)` returns `(nil, nil)` when disabled; `redis` mode requires a
non-nil rueidis client (the facade treats a disabled pulse `*redis.Client` as
absent and fails fast).

## Notes

- Deduplication is best-effort de-amplification, **not** exactly-once: two truly
  concurrent deliveries can both pass the check.
- A message with an empty `x-message-id` is never deduplicable (the consumer skips
  the check so distinct id-less records are not collapsed onto one key).
- Recommended for `unordered` + retry, so a record handled-but-not-yet-committed
  before a crash is not reprocessed.
