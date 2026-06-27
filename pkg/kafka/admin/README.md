# pkg/kafka/admin

Kafka **topic provisioning** (via franz-go `kadm`), shared by the producer and
consumer at `Start`.

## Config (`admin`)

| Key | Default | Notes |
|-----|---------|-------|
| `auto_create` | `true` | create missing topics; `false` = validate + fail fast |
| `partitions` | `4` | partition count for created topics |
| `replication_factor` | `1` | set ≥ 3 in production |
| `configs` | `{}` | extra per-topic Kafka configs applied at creation |

## API

- `Provision(ctx, cl, cfg, topics, log)` — dispatches to one of:
- `EnsureTopics(...)` — idempotently creates topics (`TopicAlreadyExists` ignored).
- `ValidateTopics(...)` — returns an error listing any topic that does not exist.

`auto_create=true` → `Provision` creates missing topics; `auto_create=false` →
`Provision` validates and returns an error for any missing topic, so a service
with externally-managed topics **fails fast at startup** instead of producing to /
consuming a topic that was never created.
