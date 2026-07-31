package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTrack TrackPath = "live/cam1/video"

// testConfig is a video track at the usual 90 kHz timescale.
func testConfig(tb testing.TB) TrackConfig {
	tb.Helper()

	return TrackConfig{
		Timescale:  90000,
		TimeSource: TimeSourceFrame,
		MIME:       "video/mp4",
		Encoding:   "fmp4",
	}
}

const (
	ticksPerGroup = 180000        // two seconds at the 90 kHz video timescale
	nanosPerGroup = 2_000_000_000 // the same two seconds in wallclock

	// wallclockBase offsets the wallclock anchors away from zero, which means
	// "no anchor" and would otherwise make group 0 untestable.
	wallclockBase = 1_700_000_000_000_000_000
)

// testGroup builds a two-second group whose media and wallclock anchors
// advance together, so both timelines stay consistent across a test.
//
// The wallclock anchor is offset from a fixed base because zero means "no
// anchor", which would make group 0 untestable for correlation.
func testGroup(tb testing.TB, sequence uint64) GroupMeta {
	tb.Helper()

	return GroupMeta{
		GroupRef:    GroupRef{Epoch: 1, Sequence: sequence},
		T0:          int64(sequence) * ticksPerGroup,
		Duration:    ticksPerGroup,
		W0:          wallclockBase + int64(sequence)*nanosPerGroup,
		ObjectCount: 60,
	}
}

// newTestWriter creates a fresh track and returns its writer along with the
// objects backing it.
func newTestWriter(tb testing.TB, opts ...WriterOption) (*Writer, *memstore.Store) {
	tb.Helper()

	objects := memstore.New()
	w, err := CreateTrack(tb.Context(), objects, testTrack, testConfig(tb), opts...)
	require.NoError(tb, err)

	return w, objects
}

func TestCreateTrack(t *testing.T) {
	objects := memstore.New()

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	root := w.Root()
	assert.Equal(t, ManifestVersion, root.Version)
	assert.Equal(t, testTrack, root.Track)
	assert.Equal(t, uint32(90000), root.Timescale)
	assert.Equal(t, TimeSourceFrame, root.TimeSource)
	assert.Equal(t, uint64(1), root.Epoch, "a fresh track starts at epoch 1 so that epoch 0 stays reserved")
	assert.Equal(t, uint64(0), root.OpenFrom)
	assert.Empty(t, root.Sealed)

	_, _, err = objects.Get(t.Context(), rootKey(testTrack))
	assert.NoError(t, err)
}

func TestCreateTrack_AlreadyExists(t *testing.T) {
	objects := memstore.New()

	_, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	_, err = CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	assert.ErrorIs(t, err, ErrTrackExists)
}

func TestCreateTrack_InvalidInput(t *testing.T) {
	tests := map[string]struct {
		track   TrackPath
		config  TrackConfig
		wantErr error
	}{
		"bad path":   {track: "/live/cam1", config: testConfig(t), wantErr: ErrInvalidTrackPath},
		"bad config": {track: testTrack, config: TrackConfig{}, wantErr: ErrInvalidGroup},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := CreateTrack(t.Context(), memstore.New(), tt.track, tt.config)
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestWriter_AppendGroup(t *testing.T) {
	w, objects := newTestWriter(t)
	payload := []byte("frames for group 0")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	assert.Equal(t, groupKey(testTrack, GroupRef{Epoch: 1, Sequence: 0}), meta.Object)
	assert.Equal(t, int64(len(payload)), meta.Size)

	stored, _, err := objects.Get(t.Context(), meta.Object)
	require.NoError(t, err)
	assert.Equal(t, payload, stored)

	data, _, err := objects.Get(t.Context(), deltaKey(testTrack, 0))
	require.NoError(t, err)

	delta, err := decodeManifest(data, func(d DeltaManifest) int { return d.Version })
	require.NoError(t, err)
	assert.Equal(t, uint64(0), delta.Seq)
	require.Len(t, delta.Groups, 1)
	assert.Equal(t, meta, delta.Groups[0])
}

func TestWriter_AppendGroup_PublishesHead(t *testing.T) {
	w, objects := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("a"))
	require.NoError(t, err)
	_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("b"))
	require.NoError(t, err)

	head, _, err := fetchHead(t.Context(), objects, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), head.Delta)
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 1}, head.Latest)
}

func TestWriter_AppendGroup_Duplicate(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("first"))
	require.NoError(t, err)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("second"))

	assert.ErrorIs(t, err, ErrGroupExists,
		"immutable group objects must refuse a rewrite rather than silently replacing data")
}

func TestWriter_AppendGroup_InvalidMeta(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), GroupMeta{GroupRef: GroupRef{Epoch: 0, Sequence: 1}}, []byte("x"))

	assert.ErrorIs(t, err, ErrInvalidGroup)
}

// Producers drop groups under congestion, so a gap is real information rather
// than corruption and must not stop the writer.
func TestWriter_AppendGroup_GappySequences(t *testing.T) {
	w, _ := newTestWriter(t)

	for _, sequence := range []uint64{0, 1, 4, 5, 9} {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err, "sequence %d", sequence)
	}

	assert.Equal(t, uint64(5), w.nextDelta, "delta numbering stays contiguous even though group sequences do not")
}

// A producer restart resets its sequence numbers. Without an epoch the reused
// sequence would collide with an immutable object and wedge the track.
func TestWriter_AppendGroup_EpochSeparatesProducerLifetimes(t *testing.T) {
	w, _ := newTestWriter(t)

	first := testGroup(t, 7)
	_, err := w.AppendGroup(t.Context(), first, []byte("before restart"))
	require.NoError(t, err)

	restarted := testGroup(t, 7)
	restarted.Epoch = 2
	second, err := w.AppendGroup(t.Context(), restarted, []byte("after restart"))
	require.NoError(t, err, "a new epoch must give the reused sequence a fresh keyspace")

	assert.NotEqual(t, first.Object, second.Object)
	assert.Equal(t, uint64(2), w.Root().Epoch, "the root advances to the epoch being written")
}

// Storing an extent rather than an endpoint is only worth it if a contradiction
// is caught: groups are serial within an epoch, so one may not start before its
// predecessor ended.
func TestWriter_AppendGroup_RejectsOverlap(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	overlapping := testGroup(t, 1)
	overlapping.T0 = ticksPerGroup - 1 // one tick before group 0 ends

	_, err = w.AppendGroup(t.Context(), overlapping, []byte("payload"))

	assert.ErrorIs(t, err, ErrGroupOutOfOrder)
}

// A gap is legal — groups get dropped — so only an overlap is a contradiction.
func TestWriter_AppendGroup_AllowsGapAfterPredecessor(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	later := testGroup(t, 5)
	later.T0 = 10 * ticksPerGroup

	_, err = w.AppendGroup(t.Context(), later, []byte("payload"))

	assert.NoError(t, err)
}

func TestWriter_AppendGroup_RejectsRewoundEpoch(t *testing.T) {
	w, _ := newTestWriter(t)

	advanced := testGroup(t, 0)
	advanced.Epoch = 3
	_, err := w.AppendGroup(t.Context(), advanced, []byte("payload"))
	require.NoError(t, err)

	stale := testGroup(t, 1)
	stale.Epoch = 2

	_, err = w.AppendGroup(t.Context(), stale, []byte("payload"))

	assert.ErrorIs(t, err, ErrGroupOutOfOrder)
}

// An epoch restarts the timeline, so the ordering check must not carry across
// one — group 0 of a new epoch legitimately precedes the old epoch's last.
func TestWriter_AppendGroup_OrderingResetsWithEpoch(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 9), []byte("payload"))
	require.NoError(t, err)

	restarted := testGroup(t, 0)
	restarted.Epoch = 2
	restarted.T0 = 0

	_, err = w.AppendGroup(t.Context(), restarted, []byte("payload"))

	assert.NoError(t, err)
}

// A track declaring frame-derived timestamps must keep an absent anchor absent
// rather than having one invented for it.
func TestWriter_AppendGroup_LeavesWallclockUnsetForFrameTracks(t *testing.T) {
	w, _ := newTestWriter(t)

	group := testGroup(t, 0)
	group.W0 = 0

	meta, err := w.AppendGroup(t.Context(), group, []byte("payload"))
	require.NoError(t, err)

	assert.False(t, meta.hasWallclock())
}

// A track declaring ledger-clock timestamps gets one stamped.
func TestWriter_AppendGroup_StampsWallclockForIngestTracks(t *testing.T) {
	objects := memstore.New()
	stamped := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)

	config := testConfig(t)
	config.TimeSource = TimeSourceIngest

	w, err := CreateTrack(t.Context(), objects, testTrack, config,
		WithClock(func() time.Time { return stamped }))
	require.NoError(t, err)

	group := testGroup(t, 0)
	group.W0 = 0

	meta, err := w.AppendGroup(t.Context(), group, []byte("payload"))
	require.NoError(t, err)

	assert.Equal(t, stamped.UnixNano(), meta.W0)
}

func TestWriter_Seal(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, w.Seal(t.Context()))

	root := w.Root()
	require.Len(t, root.Sealed, 1)
	assert.Equal(t, uint64(3), root.OpenFrom, "the open region restarts after the sealed run")

	ref := root.Sealed[0]
	assert.Equal(t, 3, ref.Groups)
	assert.Equal(t, uint64(0), ref.FirstDelta)
	assert.Equal(t, uint64(2), ref.LastDelta)

	data, _, err := objects.Get(t.Context(), ref.Key)
	require.NoError(t, err)

	sealed, err := decodeManifest(data, func(m SealedManifest) int { return m.Version })
	require.NoError(t, err)
	assert.Len(t, sealed.Groups, 3)

	// The deltas the sealed manifest replaced are redundant and reclaimed.
	for n := range uint64(3) {
		_, _, err := objects.Get(t.Context(), deltaKey(testTrack, n))
		assert.ErrorIs(t, err, store.ErrNotExist, "delta %d should have been reclaimed", n)
	}
}

// A seal whose root update fails leaves the sealed object written. Retrying it
// after more groups have arrived must not reuse that object's key: the retry
// covers a wider delta range, and a positional key would make Create return
// ErrExist, publish a root summary describing groups the object does not hold,
// and then reclaim the deltas that were their only other copy.
func TestWriter_Seal_RetryAfterFailedRootUpdate(t *testing.T) {
	objects := &FakeStore{
		SwapErrOnce: map[string]error{rootKey(testTrack): errors.New("transient failure")},
	}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.Error(t, w.Seal(t.Context()), "the first seal fails its root update")

	_, err = w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Seal(t.Context()))

	root := w.Root()
	require.Len(t, root.Sealed, 1)

	data, _, err := objects.Get(t.Context(), root.Sealed[0].Key)
	require.NoError(t, err)
	sealed, err := decodeManifest(data, func(m SealedManifest) int { return m.Version })
	require.NoError(t, err)

	assert.Equal(t, root.Sealed[0].Groups, len(sealed.Groups),
		"the root summary must match the sealed manifest it points at")

	r, err := OpenReader(t.Context(), objects, testTrack)
	require.NoError(t, err)

	var seen []uint64
	for group, err := range r.Groups(t.Context()) {
		require.NoError(t, err)
		seen = append(seen, group.Sequence)
	}

	assert.Equal(t, []uint64{0, 1, 2, 3}, seen, "no committed group may become unreachable")
}

func TestWriter_Seal_Empty(t *testing.T) {
	w, _ := newTestWriter(t)

	require.NoError(t, w.Seal(t.Context()))
	assert.Empty(t, w.Root().Sealed, "sealing an empty open region must be a no-op")
}

func TestWriter_AppendGroup_SealsAtThreshold(t *testing.T) {
	// One byte guarantees every append crosses the threshold.
	w, _ := newTestWriter(t, WithSealThreshold(1))

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	assert.Len(t, w.Root().Sealed, 1, "crossing the threshold rotates the open region")
	assert.Equal(t, uint64(1), w.Root().OpenFrom)
}

func TestOpenWriter(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(2) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	// Reopening stands in for a process restart.
	reopened, err := OpenWriter(t.Context(), objects, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), reopened.nextDelta)
	assert.Len(t, reopened.openGroups, 2)
	assert.Equal(t, uint32(90000), reopened.Root().Timescale)

	_, err = reopened.AppendGroup(t.Context(), testGroup(t, 2), []byte("payload"))
	require.NoError(t, err)

	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, 2))
	assert.NoError(t, err, "the reopened writer must continue the delta sequence, not restart it")
}

// head is a cache. Losing it must cost nothing but a probe.
func TestOpenWriter_WithoutHead(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, objects.Delete(t.Context(), headKey(testTrack)))

	reopened, err := OpenWriter(t.Context(), objects, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), reopened.nextDelta,
		"the true tip is found by probing, so a missing head loses nothing")
}

// A stale head must not stop recovery short either: probing continues past it.
func TestOpenWriter_StaleHead(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	// Rewind head to point at the very first delta.
	stale, err := encodeManifest(Head{Version: ManifestVersion, Delta: 0})
	require.NoError(t, err)

	_, currentVersion, err := objects.Get(t.Context(), headKey(testTrack))
	require.NoError(t, err)

	_, err = objects.Swap(t.Context(), headKey(testTrack), stale, currentVersion)
	require.NoError(t, err)

	reopened, err := OpenWriter(t.Context(), objects, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), reopened.nextDelta)
}

func TestOpenWriter_TrackNotFound(t *testing.T) {
	_, err := OpenWriter(t.Context(), memstore.New(), testTrack)

	assert.ErrorIs(t, err, ErrTrackNotFound)
}

// The commit is the delta write, so a failure to publish head must leave the
// group committed and the call successful.
func TestWriter_AppendGroup_HeadFailureDoesNotFailCommit(t *testing.T) {
	headFailure := errors.New("head unavailable")
	objects := &FakeStore{SwapErr: map[string]error{headKey(testTrack): headFailure}}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err, "head is a discovery cache; failing to publish it must not fail a durable commit")

	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, 0))
	assert.NoError(t, err)
	_, _, err = objects.Get(t.Context(), meta.Object)
	assert.NoError(t, err)
}

// publishHead surfaces its error rather than handling it, which is what
// makes the swallow in AppendGroup an explicit decision instead of a hidden one.
// Called directly here without the lock, which is safe in a single-goroutine
// test.
func TestWriter_publishHead(t *testing.T) {
	headFailure := errors.New("head unavailable")
	objects := &FakeStore{SwapErr: map[string]error{headKey(testTrack): headFailure}}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	err = w.publishHead(t.Context(), GroupRef{Epoch: 1, Sequence: 0})

	assert.ErrorIs(t, err, headFailure)
}

// The payload is written before the manifest, so a crash in between leaves an
// orphaned object that no reader can see — recoverable. The reverse order would
// leave a manifest pointing at nothing.
func TestWriter_AppendGroup_CommitOrder(t *testing.T) {
	objects := &FakeStore{}

	w, err := CreateTrack(t.Context(), objects, testTrack, testConfig(t))
	require.NoError(t, err)

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	_, creates, _, _ := objects.Calls()
	payloadIndex := indexOf(creates, meta.Object)
	deltaIndex := indexOf(creates, deltaKey(testTrack, 0))

	require.NotEqual(t, -1, payloadIndex)
	require.NotEqual(t, -1, deltaIndex)
	assert.Less(t, payloadIndex, deltaIndex, "the payload must land before the manifest that references it")
}

func indexOf(keys []string, want string) int {
	for i, key := range keys {
		if key == want {
			return i
		}
	}

	return -1
}
