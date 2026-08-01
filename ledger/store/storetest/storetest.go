// Package storetest provides a conformance suite for [store.Store]
// implementations.
//
// The ledger's correctness rests on details that are easy to get subtly wrong
// in a backend: that Create refuses an existing key instead of overwriting it,
// that Swap compares versions, and that deleting an absent key is not an error.
// Every backend runs the same suite so those guarantees are uniform rather than
// per-backend folklore.
//
// [Run] is generic over the backend's concrete type rather than taking a boxed
// [store.Store], so each backend instantiates its own copy of the suite
// and the assertions run against the concrete implementation.
package storetest

import (
	"context"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Run executes the conformance suite against a backend. newStore must return a
// fresh, empty store for each call.
func Run[T store.Store](t *testing.T, newStore func(t *testing.T) T) {
	t.Helper()

	t.Run("Get_Missing", func(t *testing.T) {
		backend := newStore(t)

		_, _, err := backend.Get(t.Context(), "absent")

		assert.ErrorIs(t, err, store.ErrNotExist)
	})

	t.Run("Create_ThenGet", func(t *testing.T) {
		backend := newStore(t)
		payload := []byte("frames")

		version, err := backend.Create(t.Context(), "live/cam1/groups/e000001-g00000001", payload)
		require.NoError(t, err)

		data, got, err := backend.Get(t.Context(), "live/cam1/groups/e000001-g00000001")
		require.NoError(t, err)
		assert.Equal(t, payload, data)
		assert.Equal(t, version, got, "Get must report the version Create returned")
	})

	t.Run("Create_Empty", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "empty", nil)
		require.NoError(t, err)

		data, _, err := backend.Get(t.Context(), "empty")
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	// This is what fences a superseded writer and what refuses a duplicate
	// append. A backend that overwrites here corrupts tracks silently.
	t.Run("Create_Existing", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "key", []byte("first"))
		require.NoError(t, err)

		_, err = backend.Create(t.Context(), "key", []byte("second"))
		assert.ErrorIs(t, err, store.ErrExist)

		data, _, err := backend.Get(t.Context(), "key")
		require.NoError(t, err)
		assert.Equal(t, []byte("first"), data, "a refused create must leave the original untouched")
	})

	t.Run("Swap_MatchingVersion", func(t *testing.T) {
		backend := newStore(t)

		version, err := backend.Create(t.Context(), "head", []byte("v1"))
		require.NoError(t, err)

		next, err := backend.Swap(t.Context(), "head", []byte("v2"), version)
		require.NoError(t, err)
		assert.NotEqual(t, version, next, "a swap must produce a new version")

		data, _, err := backend.Get(t.Context(), "head")
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), data)
	})

	t.Run("Swap_StaleVersion", func(t *testing.T) {
		backend := newStore(t)

		stale, err := backend.Create(t.Context(), "head", []byte("v1"))
		require.NoError(t, err)

		_, err = backend.Swap(t.Context(), "head", []byte("v2"), stale)
		require.NoError(t, err)

		_, err = backend.Swap(t.Context(), "head", []byte("v3"), stale)
		assert.ErrorIs(t, err, store.ErrVersionMismatch)

		data, _, err := backend.Get(t.Context(), "head")
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), data, "a rejected swap must not modify the object")
	})

	t.Run("Swap_AbsentWithNoVersion", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Swap(t.Context(), "head", []byte("v1"), store.NoVersion)
		require.NoError(t, err, "NoVersion means create-if-absent, which is how head is first published")

		data, _, err := backend.Get(t.Context(), "head")
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), data)
	})

	t.Run("Swap_AbsentWithVersion", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Swap(t.Context(), "head", []byte("v1"), store.Version("bogus"))

		assert.ErrorIs(t, err, store.ErrNotExist)
	})

	t.Run("Delete", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "key", []byte("data"))
		require.NoError(t, err)

		require.NoError(t, backend.Delete(t.Context(), "key"))

		_, _, err = backend.Get(t.Context(), "key")
		assert.ErrorIs(t, err, store.ErrNotExist)
	})

	// Garbage collection retries freely, so deleting twice must be safe.
	t.Run("Delete_Absent", func(t *testing.T) {
		backend := newStore(t)

		assert.NoError(t, backend.Delete(t.Context(), "never-existed"))
	})

	t.Run("Create_AfterDelete", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "key", []byte("first"))
		require.NoError(t, err)
		require.NoError(t, backend.Delete(t.Context(), "key"))

		_, err = backend.Create(t.Context(), "key", []byte("second"))
		assert.NoError(t, err, "a deleted key is free again")
	})

	t.Run("Get_ReturnsIndependentCopy", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "key", []byte("original"))
		require.NoError(t, err)

		data, _, err := backend.Get(t.Context(), "key")
		require.NoError(t, err)
		data[0] = 'X'

		again, _, err := backend.Get(t.Context(), "key")
		require.NoError(t, err)
		assert.Equal(t, []byte("original"), again, "a caller mutating its copy must not corrupt the store")
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		backend := newStore(t)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, _, err := backend.Get(ctx, "key")
		assert.Error(t, err)

		_, err = backend.Create(ctx, "key", []byte("x"))
		assert.Error(t, err)
	})
}

// ListerStore is the constraint for backends that also enumerate keys. Listing
// is optional because the read path never lists — only garbage collection does.
type ListerStore interface {
	store.Store
	store.Lister
}

// RunLister executes the enumeration conformance suite. Backends that implement
// [store.Lister] call it alongside [Run]; the rest simply do not.
func RunLister[T ListerStore](t *testing.T, newStore func(t *testing.T) T) {
	t.Helper()

	t.Run("Keys_ByPrefix", func(t *testing.T) {
		backend := newStore(t)

		for _, key := range []string{
			"live/cam1/groups/a",
			"live/cam1/groups/b",
			"live/cam1/root.manifest",
			"live/cam2/groups/a",
		} {
			_, err := backend.Create(t.Context(), key, []byte("x"))
			require.NoError(t, err)
		}

		var found []string
		for key, err := range backend.List(t.Context(), "live/cam1/groups/") {
			require.NoError(t, err)
			found = append(found, key)
		}

		assert.ElementsMatch(t, []string{"live/cam1/groups/a", "live/cam1/groups/b"}, found)
	})

	t.Run("Keys_NoMatches", func(t *testing.T) {
		backend := newStore(t)

		_, err := backend.Create(t.Context(), "live/cam1/root.manifest", []byte("x"))
		require.NoError(t, err)

		var found []string
		for key, err := range backend.List(t.Context(), "no/such/prefix/") {
			require.NoError(t, err)
			found = append(found, key)
		}

		assert.Empty(t, found)
	})
}
