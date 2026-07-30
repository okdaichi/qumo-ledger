package ledger

import (
	"context"

	"github.com/okdaichi/qumo-ledger/objectstore"
	"github.com/okdaichi/qumo-ledger/objectstore/memstore"
)

// FakeStore is an object store that behaves like a real one until told
// otherwise. It delegates to an in-memory store so that tests exercising a
// failure on one key still get correct behaviour on every other key, which is
// what makes it possible to assert that a failed head update leaves a committed
// group intact.
//
// The zero value is usable.
type FakeStore struct {
	// Inner serves every operation that is not failed. It is created on first
	// use when nil.
	Inner objectstore.Store

	// CreateErr, SwapErr and GetErr fail the matching operation for a key.
	CreateErr map[string]error
	SwapErr   map[string]error
	GetErr    map[string]error

	// Creates, Swaps and Deletes record the keys each operation was called
	// with, in call order.
	Creates []string
	Swaps   []string
	Deletes []string
}

var _ objectstore.Store = (*FakeStore)(nil)

func (s *FakeStore) inner() objectstore.Store {
	if s.Inner == nil {
		s.Inner = memstore.New()
	}

	return s.Inner
}

func (s *FakeStore) Get(ctx context.Context, key string) ([]byte, objectstore.Version, error) {
	if err := s.GetErr[key]; err != nil {
		return nil, objectstore.NoVersion, err
	}

	return s.inner().Get(ctx, key)
}

func (s *FakeStore) Create(ctx context.Context, key string, data []byte) (objectstore.Version, error) {
	s.Creates = append(s.Creates, key)
	if err := s.CreateErr[key]; err != nil {
		return objectstore.NoVersion, err
	}

	return s.inner().Create(ctx, key, data)
}

func (s *FakeStore) Swap(ctx context.Context, key string, data []byte, expect objectstore.Version) (objectstore.Version, error) {
	s.Swaps = append(s.Swaps, key)
	if err := s.SwapErr[key]; err != nil {
		return objectstore.NoVersion, err
	}

	return s.inner().Swap(ctx, key, data, expect)
}

func (s *FakeStore) Delete(ctx context.Context, key string) error {
	s.Deletes = append(s.Deletes, key)

	return s.inner().Delete(ctx, key)
}
