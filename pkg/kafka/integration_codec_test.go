//go:build integration

package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/tuanlao/pulse/pkg/kafka/codec"
	"github.com/tuanlao/pulse/pkg/log"
)

// b64JSONCodec is a non-default codec (base64 of JSON) used to prove a custom
// Deps.Codec is actually exercised end to end.
type b64JSONCodec struct{}

func (b64JSONCodec) Marshal(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := make([]byte, base64.StdEncoding.EncodedLen(len(raw)))
	base64.StdEncoding.Encode(out, raw)
	return out, nil
}

func (b64JSONCodec) Unmarshal(data []byte, v any) error {
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(raw, data)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw[:n], v)
}

func (b64JSONCodec) ContentType() string { return "application/x-pulse-b64json" }

var _ codec.Codec = b64JSONCodec{}

func TestIntegration_Codec_CustomRoundTrip(t *testing.T) {
	topic := "pulse-it-codec-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	cfg := itConfig(topic)

	got := make(chan itOrder, 1)
	hdr := make(chan *Message, 1)

	p, err := NewProducer(cfg, Deps{Logger: log.Nop(), Codec: b64JSONCodec{}}, WithBrokers(itBrokers()...))
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatalf("producer.Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })

	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), Codec: b64JSONCodec{}},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, e itOrder, m *Message) error {
		hdr <- m
		got <- e
		return nil
	})
	startConsumer(t, c)

	if err := Send(context.Background(), p, topic, "k", itOrder{ID: "codec-1", Amount: 7}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case e := <-got:
		if e.ID != "codec-1" || e.Amount != 7 {
			t.Fatalf("payload mismatch via custom codec: %+v", e)
		}
		m := <-hdr
		if m.ContentType() != "application/x-pulse-b64json" {
			t.Fatalf("x-content-type = %q, want the custom codec's", m.ContentType())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("custom-codec message not received")
	}
}

// TestIntegration_Codec_DecodeFailure_DLQ: a payload that the codec cannot decode
// is non-retryable and goes straight to the DLQ (no retries).
func TestIntegration_Codec_DecodeFailure_DLQ(t *testing.T) {
	topic := "pulse-it-baddec-" + uuid.NewString()
	group := "g-" + uuid.NewString()
	cfg := itConfig(topic)
	rec := newHookRecorder()

	p := mustProducer(t, topic) // default JSON codec
	c, err := NewConsumer(cfg, Deps{Logger: log.Nop(), Hooks: rec.hooks()},
		WithBrokers(itBrokers()...), WithGroupID(group), WithTopics(topic))
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	On(c, topic, func(_ context.Context, _ itOrder, _ *Message) error {
		t.Error("handler should not run for an undecodable payload")
		return nil
	})
	startConsumer(t, c)

	// Raw bytes that are not valid JSON for itOrder.
	m := NewMessage([]byte("k"), []byte("this is not json{"))
	if err := p.ProduceSync(context.Background(), topic, m); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	waitFor(t, 30*time.Second, func() bool { return rec.count("dlq") == 1 }, "undecodable payload DLQ'd")
	if class, _ := rec.dlq(); class != string(ErrorNonRetryable) {
		t.Fatalf("DLQ class = %q, want non_retryable", class)
	}
	if n := rec.count("retry"); n != 0 {
		t.Fatalf("decode failure must not retry, got %d retries", n)
	}
}
