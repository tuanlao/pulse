// Package message holds the public, shared types of pkg/kafka: the Message
// envelope, its typed Headers helper, the lifecycle Hooks, and the error-class
// machinery. It is a leaf package — it depends only on franz-go's kgo (for the
// record header type) and never on the other kafka sub-packages, so the facade
// can alias these types without an import cycle.
package message

import (
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Canonical Kafka header keys used across pulse. The prefix is "x-" (a neutral,
// transport-agnostic convention) rather than "pulse-" so the same envelope reads
// naturally from non-pulse consumers. Trace context travels in the standard W3C
// "traceparent"/"tracestate" headers (handled by pkg/kafka/trace), not here.
const (
	HeaderMessageID   = "x-message-id"
	HeaderRetryCount  = "x-retry-count"
	HeaderRetryGroup  = "x-retry-group"
	HeaderRetryDueAt  = "x-retry-due-at" // unix milliseconds
	HeaderOriginTopic = "x-origin-topic"
	HeaderOriginPart  = "x-origin-partition"
	HeaderOriginOff   = "x-origin-offset"
	HeaderOriginTime  = "x-origin-timestamp" // unix milliseconds
	HeaderSource      = "x-source"
	HeaderProducedAt  = "x-produced-at" // unix milliseconds
	HeaderContentType = "x-content-type"
	HeaderErrorClass  = "x-error-class"
	HeaderErrorReason = "x-error-reason"
)

// Headers wraps a record's header slice directly (no map conversion) so that
// reading a fetched record and rebuilding one to forward stays allocation-light.
// It exposes ONLY get/set accessors — it deliberately holds no business logic
// (e.g. whether an error is replayable is a policy decision owned by pkg/kafka/
// retry, not by the envelope).
type Headers struct {
	h []kgo.RecordHeader
}

// NewHeaders wraps an existing record-header slice. The slice is used as-is.
func NewHeaders(h []kgo.RecordHeader) Headers { return Headers{h: h} }

// Raw returns the underlying record-header slice (for building a *kgo.Record).
func (hd Headers) Raw() []kgo.RecordHeader { return hd.h }

// Get returns the first value for key and whether it was present.
func (hd *Headers) Get(key string) (string, bool) {
	for i := range hd.h {
		if hd.h[i].Key == key {
			return string(hd.h[i].Value), true
		}
	}
	return "", false
}

// Set replaces the value for key, or appends it when absent.
func (hd *Headers) Set(key, value string) {
	for i := range hd.h {
		if hd.h[i].Key == key {
			hd.h[i].Value = []byte(value)
			return
		}
	}
	hd.h = append(hd.h, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (hd *Headers) getInt(key string) int {
	if v, ok := hd.Get(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 0
}

func (hd *Headers) getMillis(key string) time.Time {
	if v, ok := hd.Get(key); ok {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	return time.Time{}
}

// Typed accessors. Setters mutate the wrapped slice in place.

func (hd *Headers) MessageID() string      { v, _ := hd.Get(HeaderMessageID); return v }
func (hd *Headers) SetMessageID(id string) { hd.Set(HeaderMessageID, id) }
func (hd *Headers) RetryCount() int        { return hd.getInt(HeaderRetryCount) }
func (hd *Headers) SetRetryCount(n int)    { hd.Set(HeaderRetryCount, strconv.Itoa(n)) }
func (hd *Headers) RetryGroup() string     { v, _ := hd.Get(HeaderRetryGroup); return v }
func (hd *Headers) SetRetryGroup(g string) { hd.Set(HeaderRetryGroup, g) }
func (hd *Headers) RetryDueAt() time.Time  { return hd.getMillis(HeaderRetryDueAt) }
func (hd *Headers) SetRetryDueAt(t time.Time) {
	hd.Set(HeaderRetryDueAt, strconv.FormatInt(t.UnixMilli(), 10))
}
func (hd *Headers) OriginTopic() string        { v, _ := hd.Get(HeaderOriginTopic); return v }
func (hd *Headers) SetOriginTopic(s string)    { hd.Set(HeaderOriginTopic, s) }
func (hd *Headers) OriginPartition() int       { return hd.getInt(HeaderOriginPart) }
func (hd *Headers) SetOriginPartition(p int32) { hd.Set(HeaderOriginPart, strconv.Itoa(int(p))) }
func (hd *Headers) OriginOffset() int          { return hd.getInt(HeaderOriginOff) }
func (hd *Headers) SetOriginOffset(o int64)    { hd.Set(HeaderOriginOff, strconv.FormatInt(o, 10)) }
func (hd *Headers) SetOriginTimestamp(t time.Time) {
	hd.Set(HeaderOriginTime, strconv.FormatInt(t.UnixMilli(), 10))
}
func (hd *Headers) Source() string     { v, _ := hd.Get(HeaderSource); return v }
func (hd *Headers) SetSource(s string) { hd.Set(HeaderSource, s) }
func (hd *Headers) SetProducedAt(t time.Time) {
	hd.Set(HeaderProducedAt, strconv.FormatInt(t.UnixMilli(), 10))
}
func (hd *Headers) ContentType() string     { v, _ := hd.Get(HeaderContentType); return v }
func (hd *Headers) SetContentType(s string) { hd.Set(HeaderContentType, s) }
func (hd *Headers) ErrorClass() string      { v, _ := hd.Get(HeaderErrorClass); return v }
func (hd *Headers) SetErrorClass(c string)  { hd.Set(HeaderErrorClass, c) }
func (hd *Headers) ErrorReason() string     { v, _ := hd.Get(HeaderErrorReason); return v }
func (hd *Headers) SetErrorReason(s string) { hd.Set(HeaderErrorReason, s) }
