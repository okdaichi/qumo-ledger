package fsstore_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/objectstore/fsstore"
	"github.com/okdaichi/qumo-ledger/objectstore/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Conformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) *fsstore.Store {
		store, err := fsstore.New(t.TempDir())
		require.NoError(t, err)

		return store
	})
}

func TestStore_ListerConformance(t *testing.T) {
	storetest.RunLister(t, func(t *testing.T) *fsstore.Store {
		store, err := fsstore.New(t.TempDir())
		require.NoError(t, err)

		return store
	})
}

func TestNew_CreatesRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "root")

	_, err := fsstore.New(dir)
	require.NoError(t, err)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

// Keys arrive from manifests and, in a multi-tenant deployment, potentially
// from a request. None of them may reach outside the root.
func TestStore_RejectsEscapingKeys(t *testing.T) {
	store, err := fsstore.New(t.TempDir())
	require.NoError(t, err)

	tests := map[string]string{
		"empty":           "",
		"absolute":        "/etc/passwd",
		"parent":          "../outside",
		"nested parent":   "../../outside",
		"trailing parent": "live/../../outside",
	}

	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := store.Get(t.Context(), key)
			assert.ErrorIs(t, err, fsstore.ErrInvalidKey)

			_, err = store.Create(t.Context(), key, []byte("x"))
			assert.ErrorIs(t, err, fsstore.ErrInvalidKey)
		})
	}
}

// The on-disk tree mirrors the key layout so a track can be inspected with
// ordinary tools, which is much of the point of having a local backend.
func TestStore_MapsKeysOntoPaths(t *testing.T) {
	dir := t.TempDir()
	store, err := fsstore.New(dir)
	require.NoError(t, err)

	_, err = store.Create(t.Context(), "live/cam1/video/groups/e000001-g00000042", []byte("frames"))
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(dir, "live", "cam1", "video", "groups", "e000001-g00000042"))
	require.NoError(t, err)
	assert.Equal(t, []byte("frames"), data)
}

// Swap writes through a temporary file. A failed swap must not leave one
// behind where a later listing would mistake it for an object.
func TestStore_SwapLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	store, err := fsstore.New(dir)
	require.NoError(t, err)

	version, err := store.Create(t.Context(), "head", []byte("v1"))
	require.NoError(t, err)
	_, err = store.Swap(t.Context(), "head", []byte("v2"), version)
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "head", entries[0].Name())
}
