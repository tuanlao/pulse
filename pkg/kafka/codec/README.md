# pkg/kafka/codec

Defines how typed event values are (de)serialized to and from a record's payload
bytes, so the producer's `Send` and the consumer's typed `On` register a payload
type once and let the framework marshal/unmarshal.

## API

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
    ContentType() string
}
```

- `codec.JSON` — the default (stdlib `encoding/json`, content type
  `application/json`). Stamped onto the `x-content-type` header.
- `codec.Or(c)` — returns `c`, or `JSON` when `c` is nil (keeps `Deps.Codec` optional).

Swap in protobuf / msgpack by implementing `Codec` and wiring it via
`kafka.Deps.Codec`. A leaf package — no other kafka sub-package depends on it the
other way.
