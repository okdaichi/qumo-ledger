package ledger

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/okdaichi/qumo-ledger/objectstore/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPopulatedTrack writes count groups and seals everything before sealAt, so
// the resulting track exercises both the sealed history and the open region.
func newPopulatedTrack(tb testing.TB, count, sealAt uint64) (*memstore.Store, *Writer) {
	tb.Helper()

	w, store := newTestWriter(tb)
	for sequence := range count {
		if sequence == sealAt {
			require.NoError(tb, w.Seal(tb.Context()))
		}
		_, err := w.AppendGroup(tb.Context(), testGroup(tb, sequence), []byte("payload"))
		require.NoError(tb, err)
	}

	return store, w
}

func TestOpenReader(t *testing.T) {
	store, _ := newPopulatedTrack(t, 2, 2)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	assert.Equal(t, testTrack, r.Track())
	assert.Equal(t, uint32(90000), r.Root().Timescale)
	assert.Equal(t, TimeSourceFrame, r.Root().TimeSource)
}

func TestOpenReader_TrackNotFound(t *testing.T) {
	_, err := OpenReader(t.Context(), memstore.New(), testTrack)

	assert.ErrorIs(t, err, ErrTrackNotFound)
}

func TestReader_Groups(t *testing.T) {
	store, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	var sequences []uint64
	for group, err := range r.Groups(t.Context()) {
		require.NoError(t, err)
		sequences = append(sequences, group.Sequence)
	}

	assert.Equal(t, []uint64{0, 1, 2, 3, 4}, sequences,
		"iteration must span the sealed history and the open region as one timeline")
}

func TestReader_Groups_EmptyTrack(t *testing.T) {
	_, store := newTestWriter(t)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	var groups []GroupMeta
	for group, err := range r.Groups(t.Context()) {
		require.NoError(t, err)
		groups = append(groups, group)
	}

	assert.Empty(t, groups)
}

func TestReader_ReadGroup(t *testing.T) {
	w, store := newTestWriter(t)
	payload := []byte("the frames")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	got, err := r.ReadGroup(t.Context(), meta)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestReader_Delta_NotCommitted(t *testing.T) {
	store, _ := newPopulatedTrack(t, 1, 1)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	_, err = r.Delta(t.Context(), 99)

	assert.ErrorIs(t, err, ErrNotCommitted,
		"absence is how a reader learns it has caught up, not a failure")
}

func TestReader_SeekWallclock(t *testing.T) {
	store, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]struct {
		offset   int64 // nanoseconds after the first group's anchor
		expected uint64
	}{
		"first group, in sealed history":      {offset: 500_000_000, expected: 0},
		"boundary belongs to the later group": {offset: 2_000_000_000, expected: 1},
		"last sealed group":                   {offset: 5_000_000_000, expected: 2},
		"open region":                         {offset: 7_000_000_000, expected: 3},
		"final group":                         {offset: 9_500_000_000, expected: 4},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			group, err := r.SeekWallclock(t.Context(), wallclockBase+tt.offset)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, group.Sequence)
		})
	}
}

func TestReader_SeekWallclock_OutOfRange(t *testing.T) {
	store, _ := newPopulatedTrack(t, 3, 3)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]int64{
		"before the first anchor":   wallclockBase - 1,
		"past the last group's end": wallclockBase + 999*nanosPerGroup,
	}

	for name, instant := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := r.SeekWallclock(t.Context(), instant)
			assert.ErrorIs(t, err, ErrNoGroupFound)
		})
	}
}

// Wallclock is optional. A group without an anchor cannot be correlated, so a
// wallclock seek must skip it rather than treating it as the Unix epoch.
func TestReader_SeekWallclock_SkipsGroupsWithoutAnchor(t *testing.T) {
	w, store := newTestWriter(t)

	anchored := testGroup(t, 0)
	_, err := w.AppendGroup(t.Context(), anchored, []byte("payload"))
	require.NoError(t, err)

	unanchored := testGroup(t, 1)
	unanchored.W0 = 0
	_, err = w.AppendGroup(t.Context(), unanchored, []byte("payload"))
	require.NoError(t, err)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	group, err := r.SeekWallclock(t.Context(), wallclockBase+500_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), group.Sequence)
}

func TestReader_SeekMedia(t *testing.T) {
	store, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	// 180000 ticks is two seconds at the 90 kHz video timescale.
	group, err := r.SeekMedia(t.Context(), 450000)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), group.Sequence)
}

// A reader opened before a seal keeps a stale root. Refresh is what lets it see
// history that has been rotated since.
func TestReader_Refresh(t *testing.T) {
	w, store := newTestWriter(t)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)
	require.Empty(t, r.Root().Sealed)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Seal(t.Context()))

	require.NoError(t, r.Refresh(t.Context()))
	assert.Len(t, r.Root().Sealed, 1)
}

func TestReader_Head(t *testing.T) {
	store, _ := newPopulatedTrack(t, 2, 2)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	head, err := r.Head(t.Context())
	require.NoError(t, err)

	assert.Equal(t, uint64(1), head.Delta)
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 1}, head.Latest)
}

func TestReader_Follow(t *testing.T) {
	store, _ := newPopulatedTrack(t, 3, 3)

	r, err := OpenReader(t.Context(), store, testTrack)
	require.NoError(t, err)

	var seen []uint64
	for delta, err := range r.Follow(t.Context(), 0, 10*time.Millisecond) {
		require.NoError(t, err)
		seen = append(seen, delta.Seq)
		if len(seen) == 3 {
			break
		}
	}

	assert.Equal(t, []uint64{0, 1, 2}, seen, "a backlog must drain without polling between deltas")
}

// Following past the tip must block rather than spin or error, and must stop
// when the caller's context is cancelled. synctest makes the polling delay
// deterministic instead of a real wait.
func TestReader_Follow_BlocksAtTip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, w := newPopulatedTrack(t, 1, 1)

		r, err := OpenReader(t.Context(), store, testTrack)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		received := make(chan uint64, 4)
		go func() {
			defer close(received)
			for delta, err := range r.Follow(ctx, 0, 100*time.Millisecond) {
				if err != nil {
					return
				}
				received <- delta.Seq
			}
		}()

		assert.Equal(t, uint64(0), <-received)

		// The follower is now past the tip and waiting on its ticker.
		synctest.Wait()

		_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
		require.NoError(t, err)

		assert.Equal(t, uint64(1), <-received, "a delta committed after the follower caught up must still arrive")

		cancel()
		synctest.Wait()

		_, open := <-received
		assert.False(t, open, "cancelling the context must end the iteration")
	})
}
