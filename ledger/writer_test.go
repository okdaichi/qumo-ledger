package ledger

import (
	"errors"
	"testing"

	"github.com/okdaichi/qumo-ledger/objectstore"
	"github.com/okdaichi/qumo-ledger/objectstore/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTrack TrackPath = "live/cam1/video"

func testConfig() TrackConfig {
	return TrackConfig{
		Timescale:  90000,
		TimeSource: TimeSourceFrame,
		MIME:       "video/mp4",
		Encoding:   "fmp4",
	}
}

// testGroup builds a group two seconds long at the 90 kHz video timescale,
// with a wallclock range derived from the same position so both timelines
// stay consistent across a test.
func testGroup(tb testing.TB, sequence uint64) GroupMeta {
	tb.Helper()

	const ticksPerGroup = 180000 // two seconds at 90 kHz
	const nanosPerGroup = 2_000_000_000

	return GroupMeta{
		GroupRef: GroupRef{Epoch: 1, Sequence: sequence},
		T0:       int64(sequence) * ticksPerGroup,
		T1:       int64(sequence+1) * ticksPerGroup,
		W0:       int64(sequence) * nanosPerGroup,
		W1:       int64(sequence+1) * nanosPerGroup,
	}
}

// newTestWriter creates a fresh track and returns its writer along with the
// store backing it.
func newTestWriter(tb testing.TB, opts ...WriterOption) (*Writer, *memstore.Store) {
	tb.Helper()

	store := memstore.New()
	w, err := CreateTrack(tb.Context(), store, testTrack, testConfig(), opts...)
	require.NoError(tb, err)

	return w, store
}

func TestCreateTrack(t *testing.T) {
	store := memstore.New()

	w, err := CreateTrack(t.Context(), store, testTrack, testConfig())
	require.NoError(t, err)

	root := w.Root()
	assert.Equal(t, ManifestVersion, root.Version)
	assert.Equal(t, testTrack, root.Track)
	assert.Equal(t, uint32(90000), root.Timescale)
	assert.Equal(t, TimeSourceFrame, root.TimeSource)
	assert.Equal(t, uint64(1), root.Epoch, "a fresh track starts at epoch 1 so that epoch 0 stays reserved")
	assert.Equal(t, uint64(0), root.OpenFrom)
	assert.Empty(t, root.Sealed)

	_, _, err = store.Get(t.Context(), RootKey(testTrack))
	assert.NoError(t, err)
}

func TestCreateTrack_AlreadyExists(t *testing.T) {
	store := memstore.New()

	_, err := CreateTrack(t.Context(), store, testTrack, testConfig())
	require.NoError(t, err)

	_, err = CreateTrack(t.Context(), store, testTrack, testConfig())
	assert.ErrorIs(t, err, ErrTrackExists)
}

func TestCreateTrack_InvalidInput(t *testing.T) {
	tests := map[string]struct {
		track   TrackPath
		config  TrackConfig
		wantErr error
	}{
		"bad path":   {track: "/live/cam1", config: testConfig(), wantErr: ErrInvalidTrackPath},
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
	w, store := newTestWriter(t)
	payload := []byte("frames for group 0")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	assert.Equal(t, GroupKey(testTrack, GroupRef{Epoch: 1, Sequence: 0}), meta.Object)
	assert.Equal(t, int64(len(payload)), meta.Size)

	stored, _, err := store.Get(t.Context(), meta.Object)
	require.NoError(t, err)
	assert.Equal(t, payload, stored)

	data, _, err := store.Get(t.Context(), DeltaKey(testTrack, 0))
	require.NoError(t, err)

	delta, err := decodeManifest(data, func(d DeltaManifest) int { return d.Version })
	require.NoError(t, err)
	assert.Equal(t, uint64(0), delta.Seq)
	require.Len(t, delta.Groups, 1)
	assert.Equal(t, meta, delta.Groups[0])
}

func TestWriter_AppendGroup_PublishesHead(t *testing.T) {
	w, store := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("a"))
	require.NoError(t, err)
	_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("b"))
	require.NoError(t, err)

	head, _, err := fetchHead(t.Context(), store, testTrack)
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

func TestWriter_Seal(t *testing.T) {
	w, store := newTestWriter(t)

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

	data, _, err := store.Get(t.Context(), ref.Key)
	require.NoError(t, err)

	sealed, err := decodeManifest(data, func(m SealedManifest) int { return m.Version })
	require.NoError(t, err)
	assert.Len(t, sealed.Groups, 3)

	// The deltas the sealed manifest replaced are redundant and reclaimed.
	for n := range uint64(3) {
		_, _, err := store.Get(t.Context(), DeltaKey(testTrack, n))
		assert.ErrorIs(t, err, objectstore.ErrNotExist, "delta %d should have been reclaimed", n)
	}
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
	w, store := newTestWriter(t)

	for sequence := range uint64(2) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	// Reopening stands in for a process restart.
	reopened, err := OpenWriter(t.Context(), store, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), reopened.nextDelta)
	assert.Len(t, reopened.openGroups, 2)
	assert.Equal(t, uint32(90000), reopened.Root().Timescale)

	_, err = reopened.AppendGroup(t.Context(), testGroup(t, 2), []byte("payload"))
	require.NoError(t, err)

	_, _, err = store.Get(t.Context(), DeltaKey(testTrack, 2))
	assert.NoError(t, err, "the reopened writer must continue the delta sequence, not restart it")
}

// head is a cache. Losing it must cost nothing but a probe.
func TestOpenWriter_WithoutHead(t *testing.T) {
	w, store := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, store.Delete(t.Context(), HeadKey(testTrack)))

	reopened, err := OpenWriter(t.Context(), store, testTrack)
	require.NoError(t, err)

	assert.Equal(t, uint64(3), reopened.nextDelta,
		"the true tip is found by probing, so a missing head loses nothing")
}

// A stale head must not stop recovery short either: probing continues past it.
func TestOpenWriter_StaleHead(t *testing.T) {
	w, store := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	stale, err := encodeManifest(Head{Version: ManifestVersion, Delta: 0})
	require.NoError(t, err)
	_, _, err = store.Get(t.Context(), HeadKey(testTrack))
	require.NoError(t, err)
	_, currentVersion, _ := store.Get(t.Context(), HeadKey(testTrack))
	_, err = store.Swap(t.Context(), HeadKey(testTrack), stale, currentVersion)
	require.NoError(t, err)

	reopened, err := OpenWriter(t.Context(), store, testTrack)
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
	store := &FakeStore{SwapErr: map[string]error{HeadKey(testTrack): headFailure}}

	w, err := CreateTrack(t.Context(), store, testTrack, testConfig())
	require.NoError(t, err)

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err, "head is a discovery cache; failing to publish it must not fail a durable commit")

	_, _, err = store.Get(t.Context(), DeltaKey(testTrack, 0))
	assert.NoError(t, err)
	_, _, err = store.Get(t.Context(), meta.Object)
	assert.NoError(t, err)
}

// The payload is written before the manifest, so a crash in between leaves an
// orphaned object that no reader can see — recoverable. The reverse order would
// leave a manifest pointing at nothing.
func TestWriter_AppendGroup_CommitOrder(t *testing.T) {
	store := &FakeStore{}

	w, err := CreateTrack(t.Context(), store, testTrack, testConfig())
	require.NoError(t, err)

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	payloadIndex := indexOf(store.Creates, meta.Object)
	deltaIndex := indexOf(store.Creates, DeltaKey(testTrack, 0))

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
