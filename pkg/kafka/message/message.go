package message

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Message is the envelope a handler receives and a producer sends. It carries the
// raw key/value plus the typed Headers helper and the source coordinates of a
// fetched record (Topic/Partition/Offset/Timestamp are zero for a message built
// for producing).
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
	Headers   Headers
}

// Handler processes one message. The context carries the trace id / span id and
// the request-scoped logger; returning an error triggers the configured retry /
// DLQ behavior. Wrap the error with NonRetryable to skip retries and route it
// straight to the DLQ.
type Handler func(ctx context.Context, m *Message) error

// FromRecord wraps a fetched record as a Message. The record's header slice is
// reused (not copied), so the resulting Headers reads and rebuilds cheaply.
func FromRecord(r *kgo.Record) *Message {
	return &Message{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp,
		Headers:   NewHeaders(r.Headers),
	}
}

// ToRecord builds a producible record for the given destination topic, carrying
// the message's key/value and the current header slice.
func (m *Message) ToRecord(topic string) *kgo.Record {
	return &kgo.Record{
		Topic:   topic,
		Key:     m.Key,
		Value:   m.Value,
		Headers: m.Headers.Raw(),
	}
}

// MessageID returns the deduplication id (x-message-id header).
func (m *Message) MessageID() string { return m.Headers.MessageID() }

// RetryCount returns how many times this message has been forwarded to a retry
// topic (0 on the original).
func (m *Message) RetryCount() int { return m.Headers.RetryCount() }

// RetryGroup returns the consumer group a retry is scoped to ("" = any group).
func (m *Message) RetryGroup() string { return m.Headers.RetryGroup() }

// RetryDueAt returns the time a retry-topic record becomes eligible (zero when
// unset).
func (m *Message) RetryDueAt() time.Time { return m.Headers.RetryDueAt() }

// OriginTopic returns the topic the message was first produced to (set once when
// it first enters the retry pipeline).
func (m *Message) OriginTopic() string { return m.Headers.OriginTopic() }

// Source returns the producing service identifier (x-source header).
func (m *Message) Source() string { return m.Headers.Source() }

// ContentType returns the payload content type (x-content-type header).
func (m *Message) ContentType() string { return m.Headers.ContentType() }

// NewID returns a fresh unique message id (used for deduplication).
func NewID() string { return uuid.NewString() }

// NewMessage builds a message for producing: it stamps a fresh unique message id
// (for dedup) and the produced-at timestamp.
func NewMessage(key, value []byte) *Message {
	m := &Message{Key: key, Value: value, Headers: NewHeaders(nil)}
	m.Headers.SetMessageID(NewID())
	m.Headers.SetProducedAt(time.Now())
	return m
}
