package ledger

import (
	"context"
	"slices"
	"sync"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
)

// FakeStore is an object store that behaves like a real one until told
// otherwise. It delegates to an in-memory store so that tests exercising a
// failure on one key still get correct behaviour on every other key, which is
// what makes it possible to assert that a failed head update leaves a committed
// group intact.
//
// It is safe for concurrent use, because [store.Store] requires that of every
// implementation and a follower calls it from its own goroutine. Read the
// recorded calls through [FakeStore.Calls] or [FakeStore.GetCount] rather than
// touching the slices directly, which would race a running follower.
//
// The zero value is usable.
type FakeStore struct {
	// Inner serves every operation that is not failed. It is created on first
	// use when nil.
	Inner store.Store

	// CreateErr, SwapErr and GetErr fail the matching operation for a key,
	// every time it is called. Set them before the store is shared.
	CreateErr map[string]error
	SwapErr   map[string]error
	GetErr    map[string]error

	// SwapErrOnce fails the first Swap of a key and is then consumed, which is
	// how a transient failure followed by a retry is modelled.
	SwapErrOnce map[string]error

	mu sync.Mutex
	// gets, creates, swaps and deletes record the keys each operation was
	// called with, in call order. gets is what makes read amplification
	// observable — how many objects a seek actually fetches.
	gets    []string
	creates []string
	swaps   []string
	deletes []string
}

var _ store.Store = (*FakeStore)(nil)

// Calls returns a copy of the keys recorded for each operation.
func (s *FakeStore) Calls() (gets, creates, swaps, deletes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.gets), slices.Clone(s.creates), slices.Clone(s.swaps), slices.Clone(s.deletes)
}

// GetCount reports how many recorded reads satisfy match.
func (s *FakeStore) GetCount(match func(key string) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int
	for _, key := range s.gets {
		if match(key) {
			n++
		}
	}

	return n
}

// ResetCalls discards the recorded calls, so a test can measure one phase of a
// scenario without counting its setup.
func (s *FakeStore) ResetCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.gets, s.creates, s.swaps, s.deletes = nil, nil, nil, nil
}

func (s *FakeStore) inner() store.Store {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.Inner == nil {
		s.Inner = memstore.New()
	}

	return s.Inner
}

func (s *FakeStore) record(dst *[]string, key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	*dst = append(*dst, key)
}

func (s *FakeStore) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	s.record(&s.gets, key)
	if err := s.GetErr[key]; err != nil {
		return nil, store.NoVersion, err
	}

	return s.inner().Get(ctx, key)
}

func (s *FakeStore) Create(ctx context.Context, key string, data []byte) (store.Version, error) {
	s.record(&s.creates, key)
	if err := s.CreateErr[key]; err != nil {
		return store.NoVersion, err
	}

	return s.inner().Create(ctx, key, data)
}

func (s *FakeStore) Swap(ctx context.Context, key string, data []byte, expect store.Version) (store.Version, error) {
	s.record(&s.swaps, key)
	if err := s.SwapErr[key]; err != nil {
		return store.NoVersion, err
	}

	s.mu.Lock()
	once := s.SwapErrOnce[key]
	if once != nil {
		delete(s.SwapErrOnce, key)
	}
	s.mu.Unlock()

	if once != nil {
		return store.NoVersion, once
	}

	return s.inner().Swap(ctx, key, data, expect)
}

func (s *FakeStore) Delete(ctx context.Context, key string) error {
	s.record(&s.deletes, key)

	return s.inner().Delete(ctx, key)
}
