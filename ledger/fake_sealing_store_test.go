package ledger

import (
	"context"
	"strings"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// fakeSealingStore serves a reader from the store it wraps and, the first time
// the reader fetches an open delta, seals the writer's open region first: the
// seal lands between the reader's log root and its deltas. Only the reader goes
// through it, so it is not shared between goroutines.
type fakeSealingStore struct {
	store.Store

	// writer is sealed; it writes to the wrapped store directly.
	writer *Writer
	sealed bool
}

var _ store.Store = (*fakeSealingStore)(nil)

func (s *fakeSealingStore) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	if !s.sealed && strings.Contains(key, "/"+openPrefix) {
		s.sealed = true
		if err := s.writer.Seal(ctx); err != nil {
			return nil, store.NoVersion, err
		}
	}
	return s.Store.Get(ctx, key)
}
