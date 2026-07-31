package ledger

import (
	"context"
	"iter"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPopulatedTrack writes count groups and seals everything before sealAt, so
// the resulting track exercises both the sealed history and the open region.
func newPopulatedTrack(tb testing.TB, count, sealAt uint64) (*memstore.Store, *Writer) {
	tb.Helper()

	w, objects := newTestWriter(tb)
	for sequence := range count {
		if sequence == sealAt {
			require.NoError(tb, w.Seal(tb.Context()))
		}
		_, err := w.AppendGroup(tb.Context(), testGroup(tb, sequence), []byte("payload"))
		require.NoError(tb, err)
	}

	return objects, w
}

func TestOpenReader(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 2, 2)

	r, err := OpenReader(t.Context(), objects, testTrack)
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
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
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
	_, objects := newTestWriter(t)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	var groups []GroupMeta
	for group, err := range r.Groups(t.Context()) {
		require.NoError(t, err)
		groups = append(groups, group)
	}

	assert.Empty(t, groups)
}

func TestReader_ReadGroup(t *testing.T) {
	w, objects := newTestWriter(t)
	payload := []byte("the frames")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	got, err := r.ReadGroup(t.Context(), meta)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestReader_delta_NotCommitted(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 1, 1)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	_, err = r.delta(t.Context(), 99)

	assert.ErrorIs(t, err, ErrNotCommitted,
		"absence is how a reader learns it has caught up, not a failure")
}

func TestReader_SeekWallclock(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
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
	objects, _ := newPopulatedTrack(t, 3, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
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
	w, objects := newTestWriter(t)

	anchored := testGroup(t, 0)
	_, err := w.AppendGroup(t.Context(), anchored, []byte("payload"))
	require.NoError(t, err)

	unanchored := testGroup(t, 1)
	unanchored.W0 = 0
	_, err = w.AppendGroup(t.Context(), unanchored, []byte("payload"))
	require.NoError(t, err)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	group, err := r.SeekWallclock(t.Context(), wallclockBase+500_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), group.Sequence)
}

// Seeking into the distant past must cost one sealed fetch, not one per sealed
// run — otherwise a seek gets steadily more expensive for the whole life of a
// recording, which is exactly what the summaries in the root exist to prevent.
func TestReader_SeekMedia_FetchesOneSealedManifest(t *testing.T) {
	objects := &FakeStore{}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	// Six sealed runs of one group each, plus a group left in the open region.
	for sequence := range uint64(6) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
		require.NoError(t, w.Seal(t.Context()))
	}
	_, err = w.AppendGroup(t.Context(), testGroup(t, 6), []byte("payload"))
	require.NoError(t, err)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.Len(t, r.Root().Sealed, 6)

	objects.ResetCalls()

	// Target the very first group, the worst case for a newest-first walk.
	group, err := r.SeekMedia(t.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), group.Sequence)

	fetched := objects.GetCount(func(key string) bool {
		return strings.Contains(key, "/delta/sealed-")
	})
	assert.Equal(t, 1, fetched, "a seek must fetch only the sealed run that can hold its answer")
}

// collect drains a group iterator into the sequence numbers it yielded.
func collect(tb testing.TB, seq iter.Seq2[GroupMeta, error]) []uint64 {
	tb.Helper()

	var sequences []uint64
	for group, err := range seq {
		require.NoError(tb, err)
		sequences = append(sequences, group.Sequence)
	}

	return sequences
}

func TestReader_RangeMedia(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]struct {
		from, to int64
		expected []uint64
	}{
		// Groups run [0,180k) [180k,360k) [360k,540k) [540k,720k) [720k,900k).
		"inside one group":      {from: 200_000, to: 300_000, expected: []uint64{1}},
		"straddling a boundary": {from: 270_000, to: 450_000, expected: []uint64{1, 2}},
		// A group starting before the window still overlaps it, and a player
		// needs it to decode into the window at all.
		"window opens mid-group":          {from: 400_000, to: 560_000, expected: []uint64{2, 3}},
		"whole track":                     {from: 0, to: 900_000, expected: []uint64{0, 1, 2, 3, 4}},
		"touching the lower edge exactly": {from: 180_000, to: 360_000, expected: []uint64{1}},
		"entirely before":                 {from: -100, to: 0, expected: nil},
		"entirely after":                  {from: 900_000, to: 999_999, expected: nil},
		"empty window":                    {from: 300_000, to: 300_000, expected: nil},
		"inverted window":                 {from: 500_000, to: 100_000, expected: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, collect(t, r.RangeMedia(t.Context(), tt.from, tt.to)))
		})
	}
}

func TestReader_RangeWallclock(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]struct {
		from, to int64 // nanoseconds after the first anchor
		expected []uint64
	}{
		"one group":         {from: 2_500_000_000, to: 3_500_000_000, expected: []uint64{1}},
		"spanning the seal": {from: 5_000_000_000, to: 7_000_000_000, expected: []uint64{2, 3}},
		"whole track":       {from: 0, to: 10_000_000_000, expected: []uint64{0, 1, 2, 3, 4}},
		"after the end":     {from: 20_000_000_000, to: 30_000_000_000, expected: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := collect(t, r.RangeWallclock(t.Context(), wallclockBase+tt.from, wallclockBase+tt.to))
			assert.Equal(t, tt.expected, got)
		})
	}
}

// A group with no wallclock anchor cannot be placed on the shared timeline, so
// a cross-track query must omit it rather than guess.
func TestReader_RangeWallclock_SkipsGroupsWithoutAnchor(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		group := testGroup(t, sequence)
		if sequence == 1 {
			group.W0 = 0
		}
		_, err := w.AppendGroup(t.Context(), group, []byte("payload"))
		require.NoError(t, err)
	}

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	got := collect(t, r.RangeWallclock(t.Context(), wallclockBase, wallclockBase+10*nanosPerGroup))
	assert.Equal(t, []uint64{0, 2}, got)
}

func TestReader_GroupsFrom(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]struct {
		from     GroupRef
		expected []uint64
	}{
		"from the start":                {from: GroupRef{Epoch: 1, Sequence: 0}, expected: []uint64{0, 1, 2, 3, 4}},
		"mid sealed history":            {from: GroupRef{Epoch: 1, Sequence: 2}, expected: []uint64{2, 3, 4}},
		"into the open region":          {from: GroupRef{Epoch: 1, Sequence: 4}, expected: []uint64{4}},
		"past the end":                  {from: GroupRef{Epoch: 1, Sequence: 99}, expected: nil},
		"a later epoch wins":            {from: GroupRef{Epoch: 2, Sequence: 0}, expected: nil},
		"a gap lands on the next group": {from: GroupRef{Epoch: 1, Sequence: 3}, expected: []uint64{3, 4}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, collect(t, r.GroupsFrom(t.Context(), tt.from)))
		})
	}
}

// A range must skip the sealed runs that cannot contribute, or a narrow query
// over a long recording costs as much as reading the whole thing.
func TestReader_RangeMedia_SkipsIrrelevantSealedManifests(t *testing.T) {
	objects := &FakeStore{}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	for sequence := range uint64(6) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
		require.NoError(t, w.Seal(t.Context()))
	}

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.Len(t, r.Root().Sealed, 6)

	objects.ResetCalls()

	// A window covering only group 1.
	got := collect(t, r.RangeMedia(t.Context(), ticksPerGroup, 2*ticksPerGroup))
	assert.Equal(t, []uint64{1}, got)

	fetched := objects.GetCount(func(key string) bool {
		return strings.Contains(key, "/delta/sealed-")
	})
	assert.Equal(t, 1, fetched, "only the run overlapping the window should be fetched")
}

// Manifests name their own track and range, so an object that does not match
// the key it was fetched from must be refused rather than trusted.
func TestReader_sealed_RejectsMismatchedManifest(t *testing.T) {
	tests := map[string]func(*SealedManifest){
		"wrong track": func(m *SealedManifest) { m.Track = "live/cam2/video" },
		"wrong range": func(m *SealedManifest) { m.LastDelta += 7 },
	}

	for name, corrupt := range tests {
		// Each case corrupts the stored manifest, so each needs its own objects.
		t.Run(name, func(t *testing.T) {
			w, objects := newTestWriter(t)
			for sequence := range uint64(2) {
				_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
				require.NoError(t, err)
			}
			require.NoError(t, w.Seal(t.Context()))

			r, err := OpenReader(t.Context(), objects, testTrack)
			require.NoError(t, err)
			ref := r.Root().Sealed[0]

			sealed, err := r.sealed(t.Context(), ref)
			require.NoError(t, err)
			corrupt(&sealed)

			data, err := encodeManifest(sealed)
			require.NoError(t, err)
			require.NoError(t, objects.Delete(t.Context(), ref.Key))
			_, err = objects.Create(t.Context(), ref.Key, data)
			require.NoError(t, err)

			_, err = r.sealed(t.Context(), ref)
			assert.ErrorIs(t, err, ErrManifestMismatch)
		})
	}
}

func TestOpenReader_RejectsManifestForAnotherTrack(t *testing.T) {
	objects := memstore.New()

	_, err := CreateTrack(t.Context(), objects, "live/cam1/video", testConfig(t))
	require.NoError(t, err)

	// Copy cam1's root manifest under cam2's key, as a misfiled object would be.
	data, _, err := objects.Get(t.Context(), rootKey("live/cam1/video"))
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), rootKey("live/cam2/video"), data)
	require.NoError(t, err)

	_, err = OpenReader(t.Context(), objects, "live/cam2/video")

	assert.ErrorIs(t, err, ErrManifestMismatch)
}

func TestReader_SeekMedia(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
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
	w, objects := newTestWriter(t)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)
	require.Empty(t, r.Root().Sealed)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Seal(t.Context()))

	require.NoError(t, r.Refresh(t.Context()))
	assert.Len(t, r.Root().Sealed, 1)
}

func TestReader_Head(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 2, 2)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	head, err := r.Head(t.Context())
	require.NoError(t, err)

	assert.Equal(t, uint64(1), head.Delta)
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 1}, head.Latest)
}

// The zero Cursor starts at the beginning, so a follower drains history before
// it tails.
func TestReader_Follow(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 3, 3)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	var seen []uint64
	for update, err := range r.Follow(t.Context(), Cursor{}, 10*time.Millisecond) {
		require.NoError(t, err)
		seen = append(seen, update.Sequence)
		if len(seen) == 3 {
			break
		}
	}

	assert.Equal(t, []uint64{0, 1, 2}, seen, "a backlog must drain without polling between groups")
}

// A cursor names the position *after* the group it came with, so resuming from
// one yields the next group and never repeats the last.
func TestReader_Follow_ResumesFromCursor(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 4, 4)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	var resume Cursor
	for update, err := range r.Follow(t.Context(), Cursor{}, 10*time.Millisecond) {
		require.NoError(t, err)
		if update.Sequence == 1 {
			resume = update.Cursor
			break
		}
	}

	// A restart in between: the cursor survives as text.
	encoded, err := resume.MarshalText()
	require.NoError(t, err)

	var restored Cursor
	require.NoError(t, restored.UnmarshalText(encoded))
	assert.Equal(t, resume, restored)

	var seen []uint64
	for update, err := range r.Follow(t.Context(), restored, 10*time.Millisecond) {
		require.NoError(t, err)
		seen = append(seen, update.Sequence)
		if len(seen) == 2 {
			break
		}
	}

	assert.Equal(t, []uint64{2, 3}, seen, "resuming must continue after the recorded group")
}

// Sealing deletes the deltas it folds up, so a cursor persisted before a seal
// names an object that no longer exists. The follower must serve those groups
// from the sealed run instead of waiting forever for a reclaimed delta.
func TestReader_Follow_ResumesAcrossASeal(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	// Take a cursor pointing at the second group, as a follower would persist.
	var resume Cursor
	for update, err := range r.Follow(t.Context(), Cursor{}, 10*time.Millisecond) {
		require.NoError(t, err)
		if update.Sequence == 0 {
			resume = update.Cursor
			break
		}
	}

	// While the follower is away the open region is sealed and reclaimed.
	require.NoError(t, w.Seal(t.Context()))
	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, resume.delta))
	require.ErrorIs(t, err, store.ErrNotExist, "the cursor must now point at a reclaimed delta")

	_, err = w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)

	var seen []uint64
	for update, err := range r.Follow(t.Context(), resume, 10*time.Millisecond) {
		require.NoError(t, err)
		seen = append(seen, update.Sequence)
		if update.Sequence == 3 {
			break
		}
	}

	// Replay from the start of the sealed run is within the at-least-once
	// contract; blocking, or skipping group 3, would not be.
	assert.Contains(t, seen, uint64(3), "the follower must reach groups committed after the seal")
	assert.Subset(t, []uint64{0, 1, 2, 3}, seen, "no group outside the track may appear")
}

// Tip skips history so a follower sees only what arrives next.
func TestReader_Tip(t *testing.T) {
	w, objects := newTestWriter(t)
	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	tip, err := r.Tip(t.Context())
	require.NoError(t, err)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)

	for update, err := range r.Follow(t.Context(), tip, 10*time.Millisecond) {
		require.NoError(t, err)
		assert.Equal(t, uint64(3), update.Sequence, "only the group committed after Tip should arrive")
		break
	}
}

func TestReader_Tip_EmptyTrack(t *testing.T) {
	_, objects := newTestWriter(t)

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	tip, err := r.Tip(t.Context())
	require.NoError(t, err)

	assert.Equal(t, Cursor{}, tip, "with nothing committed the start of the track is already the tip")
}

// A follower parked at the tip should cost one request per poll. Re-reading the
// root every tick doubles the request rate of every idle follower to learn
// nothing: a seal cannot happen before the delta being waited on is committed.
func TestReader_Follow_IdlePollCost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects := &FakeStore{}

		w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
		require.NoError(t, err)
		_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
		require.NoError(t, err)

		r, err := OpenReader(t.Context(), objects, testTrack)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		drained := make(chan struct{})
		go func() {
			defer close(drained)
			for _, err := range r.Follow(ctx, Cursor{}, 100*time.Millisecond) {
				if err != nil {
					return
				}
			}
		}()

		// Let the follower drain the one group and settle into polling.
		synctest.Wait()
		objects.ResetCalls()

		// Ten idle poll intervals.
		time.Sleep(1050 * time.Millisecond)
		synctest.Wait()

		probes := objects.GetCount(func(key string) bool { return strings.Contains(key, "/delta/open/") })
		roots := objects.GetCount(func(key string) bool { return strings.HasSuffix(key, "/root.manifest") })

		assert.Equal(t, 10, probes, "one probe per poll")
		assert.LessOrEqual(t, roots, 3,
			"the root is re-read only when the follower first stalls and periodically after")

		cancel()
		synctest.Wait()
		<-drained
	})
}

// Following past the tip must block rather than spin or error, and must stop
// when the caller's context is cancelled. synctest makes the polling delay
// deterministic instead of a real wait.
func TestReader_Follow_BlocksAtTip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects, w := newPopulatedTrack(t, 1, 1)

		r, err := OpenReader(t.Context(), objects, testTrack)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		received := make(chan uint64, 4)
		go func() {
			defer close(received)
			for update, err := range r.Follow(ctx, Cursor{}, 100*time.Millisecond) {
				if err != nil {
					return
				}
				received <- update.Sequence
			}
		}()

		assert.Equal(t, uint64(0), <-received)

		// The follower is now past the tip and waiting on its ticker.
		synctest.Wait()

		_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
		require.NoError(t, err)

		assert.Equal(t, uint64(1), <-received, "a group committed after the follower caught up must still arrive")

		cancel()
		synctest.Wait()

		_, open := <-received
		assert.False(t, open, "cancelling the context must end the iteration")
	})
}
