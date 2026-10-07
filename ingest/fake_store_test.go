package ingest

import (
	"context"
	"strings"
	"sync"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
)

// fakeStore is an in-memory store whose Create fails for chosen keys, so a
// test can fail one step of a ledger write and keep every other step real.
// The zero value is usable and behaves like memstore.
type fakeStore struct {
	// createErr fails a Create of every key containing a map key. Set it
	// before the store is shared.
	createErr map[string]error

	once  sync.Once
	inner *memstore.Store
}

var _ store.Store = (*fakeStore)(nil)

func (s *fakeStore) objects() *memstore.Store {
	s.once.Do(func() { s.inner = memstore.New() })
	return s.inner
}

func (s *fakeStore) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	return s.objects().Get(ctx, key)
}

func (s *fakeStore) Create(ctx context.Context, key string, data []byte) (store.Version, error) {
	for part, err := range s.createErr {
		if strings.Contains(key, part) {
			return store.NoVersion, err
		}
	}
	return s.objects().Create(ctx, key, data)
}

func (s *fakeStore) Swap(ctx context.Context, key string, data []byte, expect store.Version) (store.Version, error) {
	return s.objects().Swap(ctx, key, data, expect)
}

func (s *fakeStore) Delete(ctx context.Context, key string) error {
	return s.objects().Delete(ctx, key)
}
