package dedup

import (
	"context"
	"time"

	"github.com/maypok86/otter/v2"
)

// localDeduper remembers handled ids in an in-process otter cache (per pod). The
// TTL is fixed at construction via the write-expiry calculator.
type localDeduper struct {
	cache *otter.Cache[string, struct{}]
}

func newLocal(cfg Config) (*localDeduper, error) {
	cache, err := otter.New(&otter.Options[string, struct{}]{
		MaximumSize:      cfg.Local.MaxSize,
		ExpiryCalculator: otter.ExpiryWriting[string, struct{}](cfg.TTL),
	})
	if err != nil {
		return nil, err
	}
	return &localDeduper{cache: cache}, nil
}

func (d *localDeduper) Seen(_ context.Context, id string) (bool, error) {
	_, ok := d.cache.GetIfPresent(id)
	return ok, nil
}

func (d *localDeduper) Mark(_ context.Context, id string, _ time.Duration) error {
	d.cache.Set(id, struct{}{})
	return nil
}
