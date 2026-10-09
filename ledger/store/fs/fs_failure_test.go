package fs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The root has to be a directory. A path that is already a file cannot become
// one, and New says so rather than returning a store whose every write fails.
func TestNew_RootIsAFile(t *testing.T) {
	file := filepath.Join(t.TempDir(), "occupied")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o644))

	_, err := fs.New(file)

	assert.Error(t, err)
}

// canceledStore is a store holding one object, "kept", and a context that is
// already canceled. A canceled context stops an operation before it touches
// the disk: nothing is read, and above all nothing is written or removed on
// behalf of a caller that has already gone.
func canceledStore(tb testing.TB) (context.Context, *fs.Store) {
	tb.Helper()

	objects, err := fs.New(tb.TempDir())
	require.NoError(tb, err)
	_, err = objects.Create(tb.Context(), "kept", []byte("payload"))
	require.NoError(tb, err)

	ctx, cancel := context.WithCancel(tb.Context())
	cancel()

	return ctx, objects
}

// assertUntouched checks the store is as canceledStore left it: the existing
// object is intact and the one a canceled call would have written is absent.
func assertUntouched(tb testing.TB, objects *fs.Store) {
	tb.Helper()

	data, _, err := objects.Get(tb.Context(), "kept")
	require.NoError(tb, err)
	assert.Equal(tb, []byte("payload"), data)
	_, _, err = objects.Get(tb.Context(), "new")
	assert.ErrorIs(tb, err, store.ErrNotExist)
}

func TestStore_Get_CanceledContext(t *testing.T) {
	ctx, objects := canceledStore(t)

	_, _, err := objects.Get(ctx, "kept")

	assert.ErrorIs(t, err, context.Canceled)
}

func TestStore_Create_CanceledContext(t *testing.T) {
	ctx, objects := canceledStore(t)

	_, err := objects.Create(ctx, "new", []byte("x"))

	assert.ErrorIs(t, err, context.Canceled)
	assertUntouched(t, objects)
}

func TestStore_Swap_CanceledContext(t *testing.T) {
	ctx, objects := canceledStore(t)

	_, err := objects.Swap(ctx, "new", []byte("x"), store.NoVersion)

	assert.ErrorIs(t, err, context.Canceled)
	assertUntouched(t, objects)
}

func TestStore_Delete_CanceledContext(t *testing.T) {
	ctx, objects := canceledStore(t)

	assert.ErrorIs(t, objects.Delete(ctx, "kept"), context.Canceled)
	assertUntouched(t, objects)
}

func TestStore_List_CanceledContext(t *testing.T) {
	ctx, objects := canceledStore(t)

	var listErr error
	for _, err := range objects.List(ctx, "") {
		listErr = err
	}

	assert.ErrorIs(t, listErr, context.Canceled)
}

// Keys map onto paths, so "live/cam1" could exist beneath the object "live"
// only if "live" were both a file and a directory. The write must fail, and not
// by claiming the object already exists.
func TestStore_Create_KeyBeneathAnObject(t *testing.T) {
	objects, err := fs.New(t.TempDir())
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), "live", []byte("an object, not a prefix"))
	require.NoError(t, err)

	_, err = objects.Create(t.Context(), "live/cam1", []byte("x"))

	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrExist)
	data, _, err := objects.Get(t.Context(), "live")
	require.NoError(t, err)
	assert.Equal(t, []byte("an object, not a prefix"), data, "the object in the way is untouched")
}

func TestStore_Swap_KeyBeneathAnObject(t *testing.T) {
	objects, err := fs.New(t.TempDir())
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), "live", []byte("an object, not a prefix"))
	require.NoError(t, err)

	_, err = objects.Swap(t.Context(), "live/cam1", []byte("x"), store.NoVersion)

	assert.Error(t, err)
	data, _, err := objects.Get(t.Context(), "live")
	require.NoError(t, err)
	assert.Equal(t, []byte("an object, not a prefix"), data, "the object in the way is untouched")
}

// prefixStore is a store in which "live" names a directory of objects and is
// not itself one. Something is there, so operating on it is a failure rather
// than "not found", and must not take the object beneath it along.
func prefixStore(tb testing.TB) *fs.Store {
	tb.Helper()

	objects, err := fs.New(tb.TempDir())
	require.NoError(tb, err)
	_, err = objects.Create(tb.Context(), "live/cam1", []byte("payload"))
	require.NoError(tb, err)

	return objects
}

func TestStore_Get_KeyNamesAPrefix(t *testing.T) {
	objects := prefixStore(t)

	_, _, err := objects.Get(t.Context(), "live")

	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrNotExist)
}

func TestStore_Swap_KeyNamesAPrefix(t *testing.T) {
	objects := prefixStore(t)

	_, err := objects.Swap(t.Context(), "live", []byte("x"), store.NoVersion)

	assert.Error(t, err)
	data, _, err := objects.Get(t.Context(), "live/cam1")
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data, "the object beneath the prefix survives")
}

func TestStore_Delete_KeyNamesAPrefix(t *testing.T) {
	objects := prefixStore(t)

	assert.Error(t, objects.Delete(t.Context(), "live"))
	data, _, err := objects.Get(t.Context(), "live/cam1")
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data, "the object beneath the prefix survives")
}

// List is an iterator: a caller that has what it needs stops, and the walk
// stops with it without reporting the early exit as an error.
func TestStore_List_StopsWhenToldTo(t *testing.T) {
	objects, err := fs.New(t.TempDir())
	require.NoError(t, err)
	for _, key := range []string{"a/1", "a/2", "a/3"} {
		_, err := objects.Create(t.Context(), key, []byte("x"))
		require.NoError(t, err)
	}

	var seen int
	for _, err := range objects.List(t.Context(), "a/") {
		require.NoError(t, err)
		seen++
		break
	}

	assert.Equal(t, 1, seen)
}

// A root removed from under the store — an unmounted volume, a deleted
// directory — makes listing fail. An empty result would read as "no objects".
func TestStore_List_RootGone(t *testing.T) {
	root := filepath.Join(t.TempDir(), "store")
	objects, err := fs.New(root)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(root))

	var listErr error
	for _, err := range objects.List(t.Context(), "") {
		listErr = err
	}

	assert.Error(t, listErr)
}
