# pkg/kafka/producer

The Kafka **producer** component (`lifecycle.Component`). Publishes records with
the standard pulse headers (message id, source, content type) and a W3C produce
span. Built and wired by the `pkg/kafka` facade — use `kafka.NewProducer`.

## API

| Method | Purpose |
|--------|---------|
| `Produce(ctx, topic, *Message)` | async send; errors via the promise (OnProduceError hook + log) |
| `ProduceSync(ctx, topic, *Message) error` | synchronous send |
| `Send(ctx, topic, key string, event any) error` | marshal `event` with the codec, then send (also `kafka.Send[T]`) |
| `Start(ctx) error` | ping + provision `producer.topics` |
| `Stop(ctx) error` | flush (bounded by `flush_timeout`) then close |

## Config (`producer`)

| Key | Default | Notes |
|-----|---------|-------|
| `topics` | `[]` | topics this producer publishes to — provisioned on Start |
| `required_acks` | `all` | `leader` / `none` (these disable idempotent writes) |
| `linger` | `10ms` | batch window |
| `max_buffered_records` | `0` (kgo default) | |
| `compression` | `snappy` | `gzip` / `lz4` / `zstd` / `none` |
| `flush_timeout` | `30s` | bounds the flush on Stop |

## Topic provisioning

On `Start` the producer provisions the topics listed in `producer.topics` using
the shared `admin` config: with `admin.auto_create: true` (default) a missing
topic is **created** (4 partitions by default); with `auto_create: false` a
missing topic makes `Start` **fail fast** (returns an error → the lifecycle
manager aborts startup) so the misconfiguration is caught immediately. Declare
every topic the producer sends to — `Send` to an undeclared topic is not
provisioned.

## Notes

- The producer builds its own `*kgo.Client` (so a producer-only service does not
  pull in the consumer). Disabled (`kafka.enabled: false`) → a no-op producer.
