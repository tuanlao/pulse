package message

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHeadersGetSetRoundTrip(t *testing.T) {
	var h Headers
	h.SetMessageID("abc")
	h.SetRetryCount(2)
	h.SetRetryGroup("orders-svc")
	due := time.UnixMilli(1_700_000_000_000)
	h.SetRetryDueAt(due)
	h.SetErrorClass("non_retryable")
	h.SetErrorReason("bad body")

	if got := h.MessageID(); got != "abc" {
		t.Errorf("MessageID = %q, want abc", got)
	}
	if got := h.RetryCount(); got != 2 {
		t.Errorf("RetryCount = %d, want 2", got)
	}
	if got := h.RetryGroup(); got != "orders-svc" {
		t.Errorf("RetryGroup = %q", got)
	}
	if got := h.RetryDueAt(); !got.Equal(due) {
		t.Errorf("RetryDueAt = %v, want %v", got, due)
	}
	if got := h.ErrorClass(); got != "non_retryable" {
		t.Errorf("ErrorClass = %q", got)
	}
	if got := h.ErrorReason(); got != "bad body" {
		t.Errorf("ErrorReason = %q", got)
	}
}

func TestHeadersSetReplaces(t *testing.T) {
	var h Headers
	h.SetRetryCount(1)
	h.SetRetryCount(5)
	if got := h.RetryCount(); got != 5 {
		t.Errorf("RetryCount = %d, want 5 (replaced, not appended)", got)
	}
	if n := len(h.Raw()); n != 1 {
		t.Errorf("len(headers) = %d, want 1", n)
	}
}

func TestFromToRecordRoundTrip(t *testing.T) {
	rec := &kgo.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   []kgo.RecordHeader{{Key: HeaderMessageID, Value: []byte("id1")}},
	}
	m := FromRecord(rec)
	if m.Topic != "orders" || m.Partition != 3 || m.Offset != 42 {
		t.Fatalf("coords mismatch: %+v", m)
	}
	if m.MessageID() != "id1" {
		t.Errorf("MessageID = %q", m.MessageID())
	}
	out := m.ToRecord("orders.retry.5s")
	if out.Topic != "orders.retry.5s" || string(out.Key) != "k" || string(out.Value) != "v" {
		t.Errorf("ToRecord mismatch: %+v", out)
	}
	if len(out.Headers) != 1 || out.Headers[0].Key != HeaderMessageID {
		t.Errorf("headers not carried: %+v", out.Headers)
	}
}

func TestNonRetryable(t *testing.T) {
	base := errors.New("boom")
	wrapped := NonRetryable(base)
	if !IsNonRetryable(wrapped) {
		t.Error("IsNonRetryable(wrapped) = false")
	}
	if IsNonRetryable(base) {
		t.Error("IsNonRetryable(base) = true")
	}
	if !errors.Is(wrapped, base) {
		t.Error("errors.Is(wrapped, base) = false (Unwrap broken)")
	}
	if NonRetryable(nil) != nil {
		t.Error("NonRetryable(nil) != nil")
	}
}

func TestNewMessageStampsID(t *testing.T) {
	m := NewMessage([]byte("k"), []byte("v"))
	if m.MessageID() == "" {
		t.Error("NewMessage did not stamp a message id")
	}
}
