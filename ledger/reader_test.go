package ledger

import (
	"context"
	"errors"
	"io"
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

// openReader opens the standard test track for reading. It stands in for the
// Open + Reader pair that every read-side test starts with.
func openReader(tb testing.TB, objects store.Store) *Reader {
	tb.Helper()

	track, err := Open(tb.Context(), objects, testTrack, Config{})
	require.NoError(tb, err)
	r, err := track.Reader(tb.Context())
	require.NoError(tb, err)
	return r
}

// drain reads every currently-committed group from a Reader in order, stopping
// at io.EOF, and returns the sequence numbers.
func drain(tb testing.TB, sc *Reader) []uint64 {
	tb.Helper()

	var seqs []uint64
	for {
		g, err := sc.Next(tb.Context())
		if errors.Is(err, io.EOF) {
			return seqs
		}
		require.NoError(tb, err)
		seqs = append(seqs, g.Sequence)
	}
}

func TestTrack_Reader(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 2, 2)

	r := openReader(t, objects)

	assert.Equal(t, testTrack, r.Track())
	assert.Equal(t, uint32(90000), r.Root().Timescale)
	assert.Equal(t, TimeSourceFrame, r.Root().TimeSource)
}

func TestReader_ReadGroup(t *testing.T) {
	w, objects := newTestWriter(t)
	payload := []byte("the frames")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	r := openReader(t, objects)

	got, err := r.ReadGroup(t.Context(), meta)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func TestReader_delta_NotCommitted(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 1, 1)

	r := openReader(t, objects)

	_, err := r.delta(t.Context(), 99)

	assert.ErrorIs(t, err, ErrNotCommitted,
		"absence is how a reader learns it has caught up, not a failure")
}

func TestReader_SeekWallclock(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r := openReader(t, objects)
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

	r := openReader(t, objects)
	require.NoError(t, r.Refresh(t.Context()))

	tests := map[string]int64{
		"before the first anchor":   wallclockBase - 1,
		"past the last group's end": wallclockBase + 999*nanosPerGroup,
	}

	for name, instant := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := r.SeekWallclock(t.Context(), instant)
			assert.ErrorIs(t, err, ErrGroupNotFound)
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
	unanchored.Wallclock = 0
	_, err = w.AppendGroup(t.Context(), unanchored, []byte("payload"))
	require.NoError(t, err)

	r := openReader(t, objects)

	group, err := r.SeekWallclock(t.Context(), wallclockBase+500_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), group.Sequence)
}

// Seeking into the distant past must cost one sealed fetch, not one per sealed
// run — otherwise a seek gets steadily more expensive for the whole life of a
// recording, which is exactly what the summaries in the root exist to prevent.
func TestReader_SeekMedia_FetchesOnesealedManifest(t *testing.T) {
	objects := &FakeStore{}

	w := newWriter(t, objects, Config{})

	// Six sealed runs of one group each, plus a group left in the open region.
	for sequence := range uint64(6) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
		require.NoError(t, w.Seal(t.Context()))
	}
	_, err := w.AppendGroup(t.Context(), testGroup(t, 6), []byte("payload"))
	require.NoError(t, err)

	r := openReader(t, objects)
	require.Len(t, r.rootManifest().Sealed, 6)

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
func collect(tb testing.TB, seq iter.Seq2[GroupInfo, error]) []uint64 {
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

	r := openReader(t, objects)
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

	r := openReader(t, objects)
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
			group.Wallclock = 0
		}
		_, err := w.AppendGroup(t.Context(), group, []byte("payload"))
		require.NoError(t, err)
	}

	r := openReader(t, objects)

	got := collect(t, r.RangeWallclock(t.Context(), wallclockBase, wallclockBase+10*nanosPerGroup))
	assert.Equal(t, []uint64{0, 2}, got)
}

// A range must skip the sealed runs that cannot contribute, or a narrow query
// over a long recording costs as much as reading the whole thing.
func TestReader_RangeMedia_SkipsIrrelevantsealedManifests(t *testing.T) {
	objects := &FakeStore{}

	w := newWriter(t, objects, Config{})

	for sequence := range uint64(6) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
		require.NoError(t, w.Seal(t.Context()))
	}

	r := openReader(t, objects)
	require.Len(t, r.rootManifest().Sealed, 6)

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
	tests := map[string]func(*sealedManifest){
		"wrong track": func(m *sealedManifest) { m.Track = "live/cam2/video" },
		"wrong range": func(m *sealedManifest) { m.LastDelta += 7 },
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

			r := openReader(t, objects)
			ref := r.rootManifest().Sealed[0]

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

func TestOpen_RejectsManifestForAnotherTrack(t *testing.T) {
	objects := memstore.New()

	_, err := Create(t.Context(), objects, "live/cam1/video", testSchema(t), Config{})
	require.NoError(t, err)

	// Copy cam1's root manifest under cam2's key, as a misfiled object would be.
	data, _, err := objects.Get(t.Context(), rootKey("live/cam1/video"))
	require.NoError(t, err)
	_, err = objects.Create(t.Context(), rootKey("live/cam2/video"), data)
	require.NoError(t, err)

	// Open verifies the manifest against the key it was fetched from, so a
	// misfiled root is caught at first contact rather than trusted.
	_, err = Open(t.Context(), objects, "live/cam2/video", Config{})

	assert.ErrorIs(t, err, ErrManifestMismatch)
}

// A reader opened before a seal keeps a stale root. Refresh is what lets it see
// history that has been rotated since.
func TestReader_Refresh(t *testing.T) {
	w, objects := newTestWriter(t)

	r := openReader(t, objects)
	require.Empty(t, r.rootManifest().Sealed)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Seal(t.Context()))

	require.NoError(t, r.Refresh(t.Context()))
	assert.Len(t, r.rootManifest().Sealed, 1)
}

// --- Positioned streaming (cursor) ---------------------------------------

func TestReader_SeekStart_Drain(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	sc := openReader(t, objects)
	sc.SeekStart()

	assert.Equal(t, []uint64{0, 1, 2, 3, 4}, drain(t, sc),
		"a SeekStart drain spans sealed history and the open region as one timeline")
}

func TestReader_SeekStart_EmptyTrack(t *testing.T) {
	_, objects := newTestWriter(t)

	sc := openReader(t, objects)
	sc.SeekStart()

	_, err := sc.Next(t.Context())
	assert.ErrorIs(t, err, io.EOF)
}

// SeekGroup is exclusive: it positions strictly after the named group, so a
// resume never re-yields it.
func TestReader_SeekGroup(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3) // sealed: 0,1,2  open: 3,4

	tests := map[string]struct {
		from     GroupRef
		expected []uint64
	}{
		"from the start":            {from: GroupRef{}, expected: []uint64{0, 1, 2, 3, 4}},
		"mid sealed history":        {from: GroupRef{Epoch: 1, Sequence: 1}, expected: []uint64{2, 3, 4}},
		"into the open region":      {from: GroupRef{Epoch: 1, Sequence: 3}, expected: []uint64{4}},
		"exclusive skips the named": {from: GroupRef{Epoch: 1, Sequence: 2}, expected: []uint64{3, 4}},
		"past the end":              {from: GroupRef{Epoch: 1, Sequence: 99}, expected: nil},
		"a later epoch has nothing": {from: GroupRef{Epoch: 2, Sequence: 0}, expected: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			sc := openReader(t, objects)
			require.NoError(t, sc.SeekGroup(t.Context(), tt.from))
			assert.Equal(t, tt.expected, drain(t, sc))
		})
	}
}

// SeekMedia resolves the target group and positions the cursor there, so Next
// plays forward from it.
func TestReader_SeekMedia_Positions(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r := openReader(t, objects)

	// 450000 lands inside group 2 (360000..540000 at 90 kHz).
	g, err := r.SeekMedia(t.Context(), 450000)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), g.Sequence)
	assert.Equal(t, []uint64{2, 3, 4}, drain(t, r), "Next plays forward from the seek target")
}

func TestReader_SeekWallclock_Positions(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3)

	r := openReader(t, objects)

	// 5s after the first anchor lands in group 2.
	g, err := r.SeekWallclock(t.Context(), wallclockBase+5_000_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), g.Sequence)
	assert.Equal(t, []uint64{2, 3, 4}, drain(t, r))
}

func TestReader_SeekTip(t *testing.T) {
	objects, w := newPopulatedTrack(t, 3, 3)

	sc := openReader(t, objects)
	require.NoError(t, sc.SeekTip(t.Context()))

	// At the tip there is nothing new yet.
	_, err := sc.Next(t.Context())
	assert.ErrorIs(t, err, io.EOF)

	// A group committed after SeekTip arrives.
	_, err = w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)

	g, err := sc.Next(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), g.Sequence, "only the group committed after SeekTip should arrive")
}

func TestReader_SeekTip_EmptyTrack(t *testing.T) {
	_, objects := newTestWriter(t)

	sc := openReader(t, objects)
	require.NoError(t, sc.SeekTip(t.Context()))

	_, err := sc.Next(t.Context())
	assert.ErrorIs(t, err, io.EOF, "with nothing committed the start is already the tip")
}

// Position survives a restart as text: String it, ParseGroupRef it back, and
// SeekGroup resumes strictly after the recorded group.
func TestReader_Position_RoundTrip(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 4, 4)

	sc := openReader(t, objects)
	sc.SeekStart()

	// Process two groups, then record the position.
	require.NoError(t, drainN(t, sc, 2))
	pos := sc.Position()
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 1}, pos)

	// Persist and restore, as a restart would.
	resumed, err := ParseGroupRef(pos.String())
	require.NoError(t, err)

	// A fresh reader resumes strictly after the recorded group.
	sc2 := openReader(t, objects)
	require.NoError(t, sc2.SeekGroup(t.Context(), resumed))
	assert.Equal(t, []uint64{2, 3}, drain(t, sc2), "resuming must continue after the recorded group")
}

// drainN advances the reader n groups, failing if it hits io.EOF early.
func drainN(tb testing.TB, sc *Reader, n int) error {
	tb.Helper()
	for range n {
		if _, err := sc.Next(tb.Context()); err != nil {
			return err
		}
	}
	return nil
}

// Sealing deletes the deltas it folds up, so a Reader parked at a delta that
// is later reclaimed must still reach those groups — from the sealed run — and
// groups committed afterwards. This is the at-least-once path.
func TestReader_Next_SealReclaimRace(t *testing.T) {
	w, objects := newTestWriter(t)
	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	sc := openReader(t, objects)
	sc.SeekStart()

	// Consume group 0, parking the reader at delta 1.
	_, err := sc.Next(t.Context())
	require.NoError(t, err)

	// Seal reclaims deltas 0-2 (groups 0,1,2), deleting delta 1 the reader
	// is about to read.
	require.NoError(t, w.Seal(t.Context()))
	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, 1))
	require.ErrorIs(t, err, store.ErrNotExist, "delta 1 must be reclaimed by the seal")

	// A new group lands in the open region.
	_, err = w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)

	// The first Next after the seal refreshes and may report io.EOF once; the
	// next serves the reclaimed run (replayed from its start) and the new group.
	var seen []uint64
	for range 10 {
		g, err := sc.Next(t.Context())
		if errors.Is(err, io.EOF) {
			continue
		}
		require.NoError(t, err)
		seen = append(seen, g.Sequence)
		if g.Sequence == 3 {
			break
		}
	}

	assert.Contains(t, seen, uint64(1), "the reclaimed group must arrive from the sealed run")
	assert.Contains(t, seen, uint64(3), "the group committed after the seal must arrive")
}

// Next at the tip is non-blocking: it returns io.EOF, not a hang.
func TestReader_Next_ReturnsEOFAtTip(t *testing.T) {
	objects, w := newPopulatedTrack(t, 1, 1)

	sc := openReader(t, objects)
	sc.SeekStart()

	g, err := sc.Next(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), g.Sequence)

	_, err = sc.Next(t.Context())
	assert.ErrorIs(t, err, io.EOF, "Next at the tip returns io.EOF rather than blocking")

	_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
	require.NoError(t, err)

	g, err = sc.Next(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(1), g.Sequence, "a group committed after io.EOF still arrives")
}

// A stalled Reader should cost one probe per poll, and re-read the root only
// on the first stall and periodically after — not every tick.
func TestReader_Next_IdlePollCost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		objects := &FakeStore{}

		w := newWriter(t, objects, Config{})
		_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
		require.NoError(t, err)

		sc := openReader(t, objects)
		sc.SeekStart()
		_, err = sc.Next(t.Context()) // drain the one group
		require.NoError(t, err)

		// Settle, then start counting.
		synctest.Wait()
		objects.ResetCalls()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for range 10 {
			_, err := sc.Next(ctx)
			assert.ErrorIs(t, err, io.EOF)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
		synctest.Wait()

		probes := objects.GetCount(func(key string) bool { return strings.Contains(key, "/delta/open/") })
		roots := objects.GetCount(func(key string) bool { return strings.HasSuffix(key, "/root.manifest") })

		assert.Equal(t, 10, probes, "one probe per poll")
		assert.LessOrEqual(t, roots, 3, "the root is re-read only on the first stall and periodically after")
	})
}

// Readers are independent: each holds its own root and cursor, so a concurrent
// consumer opens its own rather than sharing one.
func TestReader_MultipleReadersIndependent(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 3, 3)

	first := openReader(t, objects)
	second := openReader(t, objects)
	first.SeekStart()
	second.SeekStart()

	assert.Equal(t, []uint64{0, 1, 2}, drain(t, first))
	assert.Equal(t, []uint64{0, 1, 2}, drain(t, second),
		"draining one reader must not consume the other's groups")
}
