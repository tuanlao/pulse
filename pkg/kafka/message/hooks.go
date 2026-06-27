package message

import (
	"context"
	"time"
)

// Hooks are optional observers of the message lifecycle. Every field is nil-safe:
// an unset hook is skipped, and a hook that panics is recovered so a buggy
// observer can never crash a worker. Pass Hooks via Deps.
type Hooks struct {
	// OnProduce fires after a record is produced (in the async promise).
	OnProduce func(ctx context.Context, m *Message)
	// OnProduceError fires when producing a record fails.
	OnProduceError func(ctx context.Context, m *Message, err error)
	// OnConsume fires just before a handler runs.
	OnConsume func(ctx context.Context, m *Message)
	// OnSuccess fires after a handler returns nil, with its duration.
	OnSuccess func(ctx context.Context, m *Message, d time.Duration)
	// OnError fires after a handler returns a non-nil error.
	OnError func(ctx context.Context, m *Message, err error)
	// OnRetry fires when a message is forwarded to a retry tier (attempt = new
	// retry count, delay = the tier's backoff).
	OnRetry func(ctx context.Context, m *Message, attempt int, delay time.Duration)
	// OnDLQ fires when a message is routed to the dead-letter queue.
	OnDLQ func(ctx context.Context, m *Message, class, reason string)
	// OnDedupeSkip fires when a message is skipped as a duplicate.
	OnDedupeSkip func(ctx context.Context, m *Message)
	// OnGroupSkip fires when a retry record scoped to another group is skipped.
	OnGroupSkip func(ctx context.Context, m *Message)
	// OnBackoffPause fires when a retry record is paused until its due time.
	OnBackoffPause func(ctx context.Context, m *Message, dueAt time.Time)
}

// recoverHook swallows a panic from a user hook (called via defer).
func recoverHook() { _ = recover() }

// The Fire* methods are the single nil-safe + panic-safe call path for each hook.

func (h Hooks) FireProduce(ctx context.Context, m *Message) {
	if h.OnProduce == nil {
		return
	}
	defer recoverHook()
	h.OnProduce(ctx, m)
}

func (h Hooks) FireProduceError(ctx context.Context, m *Message, err error) {
	if h.OnProduceError == nil {
		return
	}
	defer recoverHook()
	h.OnProduceError(ctx, m, err)
}

func (h Hooks) FireConsume(ctx context.Context, m *Message) {
	if h.OnConsume == nil {
		return
	}
	defer recoverHook()
	h.OnConsume(ctx, m)
}

func (h Hooks) FireSuccess(ctx context.Context, m *Message, d time.Duration) {
	if h.OnSuccess == nil {
		return
	}
	defer recoverHook()
	h.OnSuccess(ctx, m, d)
}

func (h Hooks) FireError(ctx context.Context, m *Message, err error) {
	if h.OnError == nil {
		return
	}
	defer recoverHook()
	h.OnError(ctx, m, err)
}

func (h Hooks) FireRetry(ctx context.Context, m *Message, attempt int, delay time.Duration) {
	if h.OnRetry == nil {
		return
	}
	defer recoverHook()
	h.OnRetry(ctx, m, attempt, delay)
}

func (h Hooks) FireDLQ(ctx context.Context, m *Message, class, reason string) {
	if h.OnDLQ == nil {
		return
	}
	defer recoverHook()
	h.OnDLQ(ctx, m, class, reason)
}

func (h Hooks) FireDedupeSkip(ctx context.Context, m *Message) {
	if h.OnDedupeSkip == nil {
		return
	}
	defer recoverHook()
	h.OnDedupeSkip(ctx, m)
}

func (h Hooks) FireGroupSkip(ctx context.Context, m *Message) {
	if h.OnGroupSkip == nil {
		return
	}
	defer recoverHook()
	h.OnGroupSkip(ctx, m)
}

func (h Hooks) FireBackoffPause(ctx context.Context, m *Message, dueAt time.Time) {
	if h.OnBackoffPause == nil {
		return
	}
	defer recoverHook()
	h.OnBackoffPause(ctx, m, dueAt)
}
