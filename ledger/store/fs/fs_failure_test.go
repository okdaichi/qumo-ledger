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

// A canceled context stops an operation before it touches the disk: nothing is
// read, and above all nothing is written or removed on behalf of a caller that
// has already gone.
func TestStore_CanceledContext(t *testing.T) {
	tests := map[string]struct {
		op func(context.Context, *fs.Store) error
	}{
		"Get": {op: func(ctx context.Context, s *fs.Store) error {
			_, _, err := s.Get(ctx, "kept")
			return err
		}},
		"Create": {op: func(ctx context.Context, s *fs.Store) error {
			_, err := s.Create(ctx, "new", []byte("x"))
			return err
		}},
		"Swap": {op: func(ctx context.Context, s *fs.Store) error {
			_, err := s.Swap(ctx, "new", []byte("x"), store.NoVersion)
			return err
		}},
		"Delete": {op: func(ctx context.Context, s *fs.Store) error {
			return s.Delete(ctx, "kept")
		}},
		"List": {op: func(ctx context.Context, s *fs.Store) error {
			for _, err := range s.List(ctx, "") {
				if err != nil {
					return err
				}
			}
			return nil
		}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			objects, err := fs.New(t.TempDir())
			require.NoError(t, err)
			_, err = objects.Create(t.Context(), "kept", []byte("payload"))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			assert.ErrorIs(t, tt.op(ctx, objects), context.Canceled)

			// The store is as it was: the existing object is intact and the
			// one the canceled call would have written does not exist.
			data, _, err := objects.Get(t.Context(), "kept")
			require.NoError(t, err)
			assert.Equal(t, []byte("payload"), data)
			_, _, err = objects.Get(t.Context(), "new")
			assert.ErrorIs(t, err, store.ErrNotExist)
		})
	}
}

// Keys map onto paths, so one object's key can be a prefix of another's only
// if the first were both a file and a directory. The second write must fail,
// and not by claiming the object already exists.
func TestStore_KeyBeneathAnObject(t *testing.T) {
	objects, err := fs.New(t.TempDir())
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), "live", []byte("an object, not a prefix"))
	require.NoError(t, err)

	_, err = objects.Create(t.Context(), "live/cam1", []byte("x"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrExist)

	_, err = objects.Swap(t.Context(), "live/cam1", []byte("x"), store.NoVersion)
	assert.Error(t, err)

	data, _, err := objects.Get(t.Context(), "live")
	require.NoError(t, err)
	assert.Equal(t, []byte("an object, not a prefix"), data, "the object in the way is untouched")
}

// A key that names a directory of objects is not itself an object. Reading it
// is a failure rather than "not found" — something is there — and neither a
// swap nor a delete may take the objects beneath it along.
func TestStore_KeyNamesAPrefix(t *testing.T) {
	objects, err := fs.New(t.TempDir())
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), "live/cam1", []byte("payload"))
	require.NoError(t, err)

	_, _, err = objects.Get(t.Context(), "live")
	require.Error(t, err)
	assert.NotErrorIs(t, err, store.ErrNotExist)

	_, err = objects.Swap(t.Context(), "live", []byte("x"), store.NoVersion)
	assert.Error(t, err)

	assert.Error(t, objects.Delete(t.Context(), "live"))

	data, _, err := objects.Get(t.Context(), "live/cam1")
	require.NoError(t, err)
	assert.Equal(t, []byte("payload"), data, "the object beneath the prefix survives all three")
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
