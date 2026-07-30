// Package memstore provides an in-memory [objectstore.Store].
//
// It implements the full contract, including compare-and-swap and listing, so
// it doubles as the reference backend: if behaviour differs between memstore
// and a real backend, the real backend is wrong.
package memstore

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/okdaichi/qumo-ledger/objectstore"
)

// Store keeps objects in a map. The zero value is not usable; call [New].
type Store struct {
	mu      sync.RWMutex
	objects map[string]object
	// nextVersion makes versions unique across the whole store rather than
	// per key, so a test cannot accidentally pass a stale version that
	// happens to match.
	nextVersion uint64
}

type object struct {
	data    []byte
	version objectstore.Version
}

var (
	_ objectstore.Store  = (*Store)(nil)
	_ objectstore.Lister = (*Store)(nil)
)

// New returns an empty Store.
func New() *Store {
	return &Store{objects: make(map[string]object)}
}

// Get implements [objectstore.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.NoVersion, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[key]
	if !ok {
		return nil, objectstore.NoVersion, fmt.Errorf("memstore: get %q: %w", key, objectstore.ErrNotExist)
	}

	return slices.Clone(obj.data), obj.version, nil
}

// Create implements [objectstore.Store].
func (s *Store) Create(ctx context.Context, key string, data []byte) (objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.NoVersion, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.objects[key]; ok {
		return objectstore.NoVersion, fmt.Errorf("memstore: create %q: %w", key, objectstore.ErrExist)
	}

	return s.storeLocked(key, data), nil
}

// Swap implements [objectstore.Store].
func (s *Store) Swap(ctx context.Context, key string, data []byte, expect objectstore.Version) (objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.NoVersion, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	obj, ok := s.objects[key]
	switch {
	case !ok && expect != objectstore.NoVersion:
		return objectstore.NoVersion, fmt.Errorf("memstore: swap %q: %w", key, objectstore.ErrNotExist)
	case ok && obj.version != expect:
		return objectstore.NoVersion, fmt.Errorf("memstore: swap %q: %w", key, objectstore.ErrVersionMismatch)
	}

	return s.storeLocked(key, data), nil
}

// Delete implements [objectstore.Store].
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.objects, key)

	return nil
}

// Keys implements [objectstore.Lister]. Keys are yielded in sorted order so
// that tests observing enumeration are deterministic; callers must not rely on
// ordering, since real backends do not guarantee it.
func (s *Store) Keys(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		s.mu.RLock()
		keys := slices.Sorted(maps.Keys(s.objects))
		s.mu.RUnlock()

		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				yield("", err)
				return
			}
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if !yield(key, nil) {
				return
			}
		}
	}
}

// Len reports how many objects are stored. It supports assertions about
// garbage collection, which is otherwise invisible from the Store interface.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.objects)
}

// storeLocked writes data under key and assigns a fresh version.
// s.mu must be held for writing.
func (s *Store) storeLocked(key string, data []byte) objectstore.Version {
	s.nextVersion++
	version := objectstore.Version(strconv.FormatUint(s.nextVersion, 10))
	s.objects[key] = object{data: slices.Clone(data), version: version}

	return version
}
