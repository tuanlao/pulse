// Package codec defines how typed event values are (de)serialized to and from a
// Kafka record's payload bytes. It lets the producer's Send and the consumer's
// typed On register a payload type once and let the framework marshal/unmarshal,
// instead of every service hand-rolling encoding. The default is JSON; a service
// can swap in protobuf or msgpack by implementing Codec and wiring it via
// Deps.Codec. It is a leaf package (no other kafka sub-package depends on it the
// other way around).
package codec

import "encoding/json"

// Codec marshals and unmarshals event values. ContentType is stamped onto the
// record's x-content-type header so consumers can tell payloads apart.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	ContentType() string
}

// JSON is the default Codec, backed by encoding/json.
var JSON Codec = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) ContentType() string                { return "application/json" }

// Or returns c when non-nil, otherwise the JSON default. Constructors use it to
// keep Deps.Codec optional.
func Or(c Codec) Codec {
	if c == nil {
		return JSON
	}
	return c
}
