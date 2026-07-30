package memstore_test

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/objectstore"
	"github.com/okdaichi/qumo-ledger/objectstore/memstore"
	"github.com/okdaichi/qumo-ledger/objectstore/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Conformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) objectstore.Store {
		return memstore.New()
	})
}

func TestStore_Len(t *testing.T) {
	store := memstore.New()
	assert.Equal(t, 0, store.Len())

	_, err := store.Create(t.Context(), "a", []byte("x"))
	require.NoError(t, err)
	_, err = store.Create(t.Context(), "b", []byte("y"))
	require.NoError(t, err)
	assert.Equal(t, 2, store.Len())

	require.NoError(t, store.Delete(t.Context(), "a"))
	assert.Equal(t, 1, store.Len())
}

// Versions are unique across the store rather than per key, so a version taken
// from one object can never accidentally satisfy a swap on another.
func TestStore_VersionsAreStoreWide(t *testing.T) {
	store := memstore.New()

	first, err := store.Create(t.Context(), "a", []byte("same"))
	require.NoError(t, err)
	second, err := store.Create(t.Context(), "b", []byte("same"))
	require.NoError(t, err)

	assert.NotEqual(t, first, second, "identical content under different keys must not share a version")
}
