package mem_test

import (
	"context"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The memory store never blocks, but it honors cancellation all the same so
// that code tested against it meets the behavior of the backends that do.
func TestStore_CanceledContext(t *testing.T) {
	tests := map[string]struct {
		op func(context.Context, *mem.Store) error
	}{
		"Get": {op: func(ctx context.Context, s *mem.Store) error {
			_, _, err := s.Get(ctx, "kept")
			return err
		}},
		"Create": {op: func(ctx context.Context, s *mem.Store) error {
			_, err := s.Create(ctx, "new", []byte("x"))
			return err
		}},
		"Swap": {op: func(ctx context.Context, s *mem.Store) error {
			_, err := s.Swap(ctx, "new", []byte("x"), store.NoVersion)
			return err
		}},
		"Delete": {op: func(ctx context.Context, s *mem.Store) error {
			return s.Delete(ctx, "kept")
		}},
		"List": {op: func(ctx context.Context, s *mem.Store) error {
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
			objects := mem.New()
			_, err := objects.Create(t.Context(), "kept", []byte("payload"))
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			assert.ErrorIs(t, tt.op(ctx, objects), context.Canceled)

			data, _, err := objects.Get(t.Context(), "kept")
			require.NoError(t, err)
			assert.Equal(t, []byte("payload"), data)
			assert.Equal(t, 1, objects.Len(), "a canceled call neither adds nor removes an object")
		})
	}
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
