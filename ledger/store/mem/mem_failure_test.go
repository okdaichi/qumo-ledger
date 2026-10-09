package mem_test

import (
	"context"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// canceledStore is a store holding one object, "kept", and a context that is
// already canceled. The memory store never blocks, but it honors cancellation
// all the same so that code tested against it meets the behavior of the
// backends that do.
func canceledStore(tb testing.TB) (context.Context, *mem.Store) {
	tb.Helper()

	objects := mem.New()
	_, err := objects.Create(tb.Context(), "kept", []byte("payload"))
	require.NoError(tb, err)

	ctx, cancel := context.WithCancel(tb.Context())
	cancel()

	return ctx, objects
}

// assertUntouched checks the store is as canceledStore left it.
func assertUntouched(tb testing.TB, objects *mem.Store) {
	tb.Helper()

	data, _, err := objects.Get(tb.Context(), "kept")
	require.NoError(tb, err)
	assert.Equal(tb, []byte("payload"), data)
	assert.Equal(tb, 1, objects.Len(), "a canceled call neither adds nor removes an object")
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

func TestStore_List_StopsWhenToldTo(t *testing.T) {
	objects := mem.New()
	for _, key := range []string{"a/1", "a/2", "b/1"} {
		_, err := objects.Create(t.Context(), key, []byte("x"))
		require.NoError(t, err)
	}

	var seen []string
	for key, err := range objects.List(t.Context(), "a/") {
		require.NoError(t, err)
		seen = append(seen, key)
		break
	}

	assert.Equal(t, []string{"a/1"}, seen)
}

// The key set is snapshotted before the first yield, so a caller may write to
// the store from inside the loop without deadlocking on the store's own lock.
func TestStore_List_AllowsWritesWhileIterating(t *testing.T) {
	objects := mem.New()
	for _, key := range []string{"a/1", "a/2"} {
		_, err := objects.Create(t.Context(), key, []byte("x"))
		require.NoError(t, err)
	}

	var seen []string
	for key, err := range objects.List(t.Context(), "a/") {
		require.NoError(t, err)
		seen = append(seen, key)
		require.NoError(t, objects.Delete(t.Context(), key))
	}

	assert.Equal(t, []string{"a/1", "a/2"}, seen)
	assert.Equal(t, 0, objects.Len())
}
