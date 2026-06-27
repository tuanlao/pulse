# pkg/kafka/metrics

Package-owned Prometheus collectors for `pkg/kafka`. Following pulse's registry
discipline, `NewMetrics(cfg, reg)` registers into a **provided** registry
(typically the server's, so one `/metrics` exposes everything) — never the global
default — and returns `nil` when disabled.

## Config (`metrics`)

| Key | Default |
|-----|---------|
| `enabled` | `true` |
| `namespace` | `pulse` |
| `subsystem` | `kafka` |
| `buckets` | `[0.001 … 30]` (seconds) |

## Collectors

`produce_total{topic,status}`, `produce_duration_seconds{topic,status}`,
`consume_total{topic,group,status}` (status: success/error/retry/dlq/dedup_skip/group_skip),
`consume_duration_seconds{topic,group}`, `retries_total{topic,group}`,
`dlq_total{topic,group,class}`, `dedup_skipped_total{topic,group}`,
`group_skipped_total{topic,group}`, `backoff_paused{topic,group}` (gauge),
`handler_in_flight{topic,group}` (gauge).

Label cardinality is bounded to topic / group / status / class — never keys,
partitions, or message ids. All record methods are nil-safe (a nil `*Metrics`
no-ops), so a disabled metrics set needs no call-site branching.
