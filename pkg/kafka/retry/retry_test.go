package retry

import (
	"testing"
	"time"

	"github.com/tuanlao/pulse/pkg/kafka/message"
)

func defaulted() Config {
	c := DefaultConfig()
	c.ApplyDefaults()
	return c
}

func msgWithRetryCount(topic string, count int) *message.Message {
	m := &message.Message{Topic: topic, Headers: message.NewHeaders(nil)}
	if count > 0 {
		m.Headers.SetRetryCount(count)
	}
	return m
}

func TestPlanner(t *testing.T) {
	cfg := defaulted() // backoffs 5s/10s/1m, maxAttempts 3
	p := NewPlanner(cfg, "g")
	now := time.UnixMilli(1_700_000_000_000)

	tests := []struct {
		name      string
		count     int
		class     message.ErrorClass
		wantDLQ   bool
		wantTopic string
		wantClass message.ErrorClass
		wantDue   time.Duration
	}{
		{"first retry -> 5s tier", 0, "", false, "orders.retry.5s", "", 5 * time.Second},
		{"second retry -> 10s tier", 1, "", false, "orders.retry.10s", "", 10 * time.Second},
		{"third retry -> 1m tier", 2, "", false, "orders.retry.1m", "", time.Minute},
		{"exhausted -> dlq", 3, "", true, "orders.dlq", message.ErrorRetriesExhausted, 0},
		{"non-retryable -> dlq immediately", 0, message.ErrorNonRetryable, true, "orders.dlq", message.ErrorNonRetryable, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := p.Plan(msgWithRetryCount("orders", tc.count), tc.class, now)
			if a.IsDLQ != tc.wantDLQ {
				t.Fatalf("IsDLQ = %v, want %v", a.IsDLQ, tc.wantDLQ)
			}
			if a.Target != tc.wantTopic {
				t.Errorf("Target = %q, want %q", a.Target, tc.wantTopic)
			}
			if a.Class != tc.wantClass {
				t.Errorf("Class = %q, want %q", a.Class, tc.wantClass)
			}
			if !tc.wantDLQ {
				if a.Attempt != tc.count+1 {
					t.Errorf("Attempt = %d, want %d", a.Attempt, tc.count+1)
				}
				if got := a.DueAt.Sub(now); got != tc.wantDue {
					t.Errorf("DueAt delta = %v, want %v", got, tc.wantDue)
				}
			}
		})
	}
}

func TestFormatDelay(t *testing.T) {
	tests := []struct {
		d      time.Duration
		format string
		want   string
	}{
		{5 * time.Second, "human", "5s"},
		{10 * time.Second, "human", "10s"},
		{time.Minute, "human", "1m"},
		{time.Hour, "human", "1h"},
		{90 * time.Second, "human", "90s"},
		{5 * time.Second, "ms", "5000"},
		{time.Minute, "ms", "60000"},
	}
	for _, tc := range tests {
		if got := FormatDelay(tc.d, tc.format); got != tc.want {
			t.Errorf("FormatDelay(%v,%q) = %q, want %q", tc.d, tc.format, got, tc.want)
		}
	}
}

func TestNamer(t *testing.T) {
	n := NewNamer(defaulted())
	tiers := n.RetryTopics("orders", "g")
	want := []string{"orders.retry.5s", "orders.retry.10s", "orders.retry.1m"}
	if len(tiers) != len(want) {
		t.Fatalf("RetryTopics = %v", tiers)
	}
	for i := range want {
		if tiers[i] != want[i] {
			t.Errorf("tier[%d] = %q, want %q", i, tiers[i], want[i])
		}
	}
	if got := n.DLQTopic("orders", "g"); got != "orders.dlq" {
		t.Errorf("DLQTopic = %q", got)
	}
}

func TestNamerGroupToken(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RetrySuffixPattern = "{origin}.retry.{group}.{delay}"
	cfg.ApplyDefaults()
	n := NewNamer(cfg)
	if got := n.RetryTopic("orders", 5*time.Second, "g1"); got != "orders.retry.g1.5s" {
		t.Errorf("RetryTopic = %q, want orders.retry.g1.5s", got)
	}
}

func TestReplayPolicy(t *testing.T) {
	if !Replayable(message.ErrorRetriesExhausted) {
		t.Error("retries_exhausted should be replayable")
	}
	if Replayable(message.ErrorNonRetryable) {
		t.Error("non_retryable should not be replayable")
	}
}

func TestEffectiveStrategy(t *testing.T) {
	auto := DefaultConfig()
	auto.ApplyDefaults()
	if got := EffectiveStrategy(auto, "unordered"); got != StrategyNonBlocking {
		t.Errorf("auto+unordered = %q, want non_blocking", got)
	}
	if got := EffectiveStrategy(auto, "ordered"); got != StrategyBlocking {
		t.Errorf("auto+ordered = %q, want blocking", got)
	}
	forced := DefaultConfig()
	forced.Strategy = StrategyBlocking
	forced.ApplyDefaults()
	if got := EffectiveStrategy(forced, "unordered"); got != StrategyBlocking {
		t.Errorf("forced blocking+unordered = %q, want blocking", got)
	}
}

func TestApplyDefaultsClampsMaxAttempts(t *testing.T) {
	c := DefaultConfig()
	c.MaxAttempts = 99 // more than len(backoffs)
	c.ApplyDefaults()
	if c.MaxAttempts != len(c.Backoffs) {
		t.Errorf("MaxAttempts = %d, want clamped to %d", c.MaxAttempts, len(c.Backoffs))
	}
}
