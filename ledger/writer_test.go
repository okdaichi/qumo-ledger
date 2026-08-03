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

// testSchema is a video track at the usual 90 kHz timescale.
func testSchema(tb testing.TB) TrackSchema {
	tb.Helper()

	return TrackSchema{
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
// anchor", which would make group 0 untestable for correlation. Only the ID's
// sequence is read by AppendGroup; the epoch is stamped from the writer.
func testGroup(tb testing.TB, sequence uint64) GroupInfo {
	tb.Helper()

	return GroupInfo{
		ID:          NewGroupID(1, sequence),
		MediaTime:   int64(sequence) * ticksPerGroup,
		Duration:    ticksPerGroup,
		Wallclock:   wallclockBase + int64(sequence)*nanosPerGroup,
		ObjectCount: 60,
	}
}

// newTestWriter creates a fresh track and returns its writer along with the
// objects backing it.
func newTestWriter(tb testing.TB) (*Writer, *memstore.Store) {
	tb.Helper()

	objects := memstore.New()
	return newWriter(tb, objects, Config{}), objects
}

// newWriter creates the standard test track and returns its writer, for the
// cases that need a fake store or non-default settings. newTestWriter covers
// the common fresh-memstore case.
func newWriter(tb testing.TB, objects store.Store, cfg Config) *Writer {
	tb.Helper()

	track, err := Create(tb.Context(), objects, testTrack, testSchema(tb), cfg)
	require.NoError(tb, err)

	w, err := track.Writer(tb.Context())
	require.NoError(tb, err)

	return w
}

func TestCreate(t *testing.T) {
	objects := memstore.New()

	track, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	require.NoError(t, err)

	root := track.root
	assert.Equal(t, manifestVersion, root.Version)
	assert.Equal(t, testTrack, root.Track)
	assert.Equal(t, uint32(90000), root.Timescale)
	assert.Equal(t, TimeSourceFrame, root.TimeSource)
	assert.Equal(t, uint64(1), root.LatestEpoch, "a fresh track starts at epoch 1 so epoch 0 stays reserved")

	_, _, err = objects.Get(t.Context(), rootKey(testTrack))
	assert.NoError(t, err, "the track root is durable")

	// Create eagerly writes the first epoch's log, so epoch 1 is writable at once.
	log, _, err := fetchEpochLog(t.Context(), objects, testTrack, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), log.Epoch)
	assert.Equal(t, uint64(0), log.OpenFrom)
	assert.Empty(t, log.Sealed)
}

func TestCreate_AlreadyExists(t *testing.T) {
	objects := memstore.New()

	_, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	require.NoError(t, err)

	_, err = Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	assert.ErrorIs(t, err, ErrTrackExists)
}

// Every Config field means a documented default when left zero, so the smallest
// usable track is an empty Config.
func TestCreate_Defaults(t *testing.T) {
	track, err := Create(t.Context(), memstore.New(), testTrack, testSchema(t), Config{})
	require.NoError(t, err)

	assert.Equal(t, int64(DefaultSealThreshold), track.sealThreshold)
	assert.NotNil(t, track.clock)
	assert.NotNil(t, track.logger)
}

func TestCreate_InvalidInput(t *testing.T) {
	tests := map[string]struct {
		track   TrackPath
		config  TrackSchema
		wantErr error
	}{
		"bad path":   {track: "/live/cam1", config: testSchema(t), wantErr: ErrInvalidTrackPath},
		"bad config": {track: testTrack, config: TrackSchema{}, wantErr: ErrInvalidGroup},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Create(t.Context(), memstore.New(), tt.track, tt.config, Config{})
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// Append is the sequential default: it derives sequence, media time, and
// wallclock so a back-to-back producer hands the ledger only a duration.
func TestWriter_Append(t *testing.T) {
	objects := memstore.New()
	now := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)

	track, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{Clock: func() time.Time { return now }})
	require.NoError(t, err)
	w, err := track.Writer(t.Context())
	require.NoError(t, err)

	first, err := w.Append(t.Context(), ticksPerGroup, []byte("a"))
	require.NoError(t, err)

	assert.Equal(t, NewGroupID(1, 0), first.ID)
	assert.Equal(t, int64(0), first.MediaTime, "the first group anchors at the start of the timeline")
	assert.Equal(t, int64(ticksPerGroup), first.Duration)
	assert.Equal(t, now.UnixNano(), first.Wallclock, "wallclock comes from the writer's clock")

	// Advance the clock so the second group's wallclock differs and is testable.
	now = now.Add(nanosPerGroup)
	second, err := w.Append(t.Context(), ticksPerGroup, []byte("b"))
	require.NoError(t, err)

	assert.Equal(t, NewGroupID(1, 1), second.ID, "sequence increments by one")
	assert.Equal(t, first.MediaTime+ticksPerGroup, second.MediaTime, "media time advances by the previous duration")
	assert.Equal(t, now.UnixNano(), second.Wallclock)
}

func TestWriter_AppendGroup(t *testing.T) {
	w, objects := newTestWriter(t)
	payload := []byte("frames for group 0")

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), payload)
	require.NoError(t, err)

	assert.Equal(t, groupKey(testTrack, NewGroupID(1, 0)), meta.ObjectKey)
	assert.Equal(t, int64(len(payload)), meta.Size)

	stored, _, err := objects.Get(t.Context(), meta.ObjectKey)
	require.NoError(t, err)
	assert.Equal(t, payload, stored)

	data, _, err := objects.Get(t.Context(), deltaKey(testTrack, 1, 0))
	require.NoError(t, err)

	delta, err := decodeManifest(data, func(d deltaManifest) int { return d.Version })
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

	head, _, err := fetchHead(t.Context(), objects, testTrack, 1)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), head.Delta)
	assert.Equal(t, NewGroupID(1, 1), head.Latest)
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

	_, err := w.AppendGroup(t.Context(), GroupInfo{ID: NewGroupID(1, 1), Duration: -1}, []byte("x"))

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

// A producer restart resets its sequence numbers. Each epoch is its own
// keyspace, so a reused sequence under a new epoch gets a fresh object rather
// than colliding with the immutable one from the previous lifetime.
func TestWriter_NewEpoch_SeparatesProducerLifetimes(t *testing.T) {
	objects := memstore.New()
	track, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	require.NoError(t, err)

	w, err := track.Writer(t.Context())
	require.NoError(t, err)
	first := testGroup(t, 7)
	_, err = w.AppendGroup(t.Context(), first, []byte("before restart"))
	require.NoError(t, err)

	// A new epoch is a new producer lifetime with its own keyspace.
	require.NoError(t, w.NewEpoch(t.Context()))

	// The same sequence under the new epoch must get a fresh object rather than
	// collide with the immutable object from the previous lifetime.
	second, err := w.AppendGroup(t.Context(), testGroup(t, 7), []byte("after restart"))
	require.NoError(t, err)

	assert.NotEqual(t, first.ObjectKey, second.ObjectKey)
	assert.Equal(t, uint64(2), w.Epoch(), "NewEpoch advances the writer's epoch")
	assert.Equal(t, uint64(2), track.LatestEpoch(), "and the track's")
}

// Storing an extent rather than an endpoint is only worth it if a contradiction
// is caught: groups are serial within an epoch, so one may not start before its
// predecessor ended.
func TestWriter_AppendGroup_RejectsOverlap(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	overlapping := testGroup(t, 1)
	overlapping.MediaTime = ticksPerGroup - 1 // one tick before group 0 ends

	_, err = w.AppendGroup(t.Context(), overlapping, []byte("payload"))

	assert.ErrorIs(t, err, ErrGroupOutOfOrder)
}

// A gap is legal — groups get dropped — so only an overlap is a contradiction.
func TestWriter_AppendGroup_AllowsGapAfterPredecessor(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	later := testGroup(t, 5)
	later.MediaTime = 10 * ticksPerGroup

	_, err = w.AppendGroup(t.Context(), later, []byte("payload"))

	assert.NoError(t, err)
}

// A new epoch restarts the timeline, so the ordering check must not carry across
// one — group 0 of a new epoch legitimately precedes the old epoch's last.
func TestWriter_NewEpoch_OrderingResets(t *testing.T) {
	w, _ := newTestWriter(t)

	_, err := w.AppendGroup(t.Context(), testGroup(t, 9), []byte("payload"))
	require.NoError(t, err)

	require.NoError(t, w.NewEpoch(t.Context()))

	restarted := testGroup(t, 0)
	restarted.MediaTime = 0

	_, err = w.AppendGroup(t.Context(), restarted, []byte("payload"))

	assert.NoError(t, err, "a new epoch restarts the timeline, so group 0 may precede the old epoch's last")
}

// NewEpoch persists a new epoch that survives a reopen, and a reused sequence
// under it lands in a fresh keyspace.
func TestWriter_NewEpoch(t *testing.T) {
	objects := memstore.New()
	track, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	require.NoError(t, err)
	w, err := track.Writer(t.Context())
	require.NoError(t, err)

	first, err := w.AppendGroup(t.Context(), testGroup(t, 3), []byte("before"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), w.Epoch())

	// The next producer lifetime is a new epoch.
	require.NoError(t, w.NewEpoch(t.Context()))
	require.Equal(t, uint64(2), w.Epoch(), "NewEpoch advances the epoch immediately")

	second, err := w.AppendGroup(t.Context(), testGroup(t, 3), []byte("after"))
	require.NoError(t, err)
	assert.NotEqual(t, first.ObjectKey, second.ObjectKey)

	// The advanced epoch persists across a reopen.
	reopened, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), reopened.LatestEpoch(), "the advanced epoch must be durable")

	// Both keys resolve.
	for _, meta := range []GroupInfo{first, second} {
		_, _, err := objects.Get(t.Context(), meta.ObjectKey)
		assert.NoError(t, err, "group %s must be readable", meta.ID)
	}
}

// A track declaring frame-derived timestamps must keep an absent anchor absent
// rather than having one invented for it.
func TestWriter_AppendGroup_LeavesWallclockUnsetForFrameTracks(t *testing.T) {
	w, _ := newTestWriter(t)

	group := testGroup(t, 0)
	group.Wallclock = 0

	meta, err := w.AppendGroup(t.Context(), group, []byte("payload"))
	require.NoError(t, err)

	assert.False(t, meta.hasWallclock())
}

// A track declaring ledger-clock timestamps gets one stamped.
func TestWriter_AppendGroup_StampsWallclockForIngestTracks(t *testing.T) {
	objects := memstore.New()
	stamped := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)

	config := testSchema(t)
	config.TimeSource = TimeSourceIngest

	track, err := Create(t.Context(), objects, testTrack, config, Config{Clock: func() time.Time { return stamped }})
	require.NoError(t, err)
	w, err := track.Writer(t.Context())
	require.NoError(t, err)

	group := testGroup(t, 0)
	group.Wallclock = 0

	meta, err := w.AppendGroup(t.Context(), group, []byte("payload"))
	require.NoError(t, err)

	assert.Equal(t, stamped.UnixNano(), meta.Wallclock)
}

func TestWriter_Seal(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, w.Seal(t.Context()))

	root := w.logRootCopy()
	require.Len(t, root.Sealed, 1)
	assert.Equal(t, uint64(3), root.OpenFrom, "the open region restarts after the sealed run")

	ref := root.Sealed[0]
	assert.Equal(t, 3, ref.Groups)
	assert.Equal(t, uint64(0), ref.FirstDelta)
	assert.Equal(t, uint64(2), ref.LastDelta)

	data, _, err := objects.Get(t.Context(), ref.Key)
	require.NoError(t, err)

	sealed, err := decodeManifest(data, func(m sealedManifest) int { return m.Version })
	require.NoError(t, err)
	assert.Len(t, sealed.Groups, 3)

	// The deltas the sealed manifest replaced are redundant and reclaimed.
	for n := range uint64(3) {
		_, _, err := objects.Get(t.Context(), deltaKey(testTrack, 1, n))
		assert.ErrorIs(t, err, store.ErrNotExist, "delta %d should have been reclaimed", n)
	}
}

// A seal whose log-root update fails leaves the sealed object written. Retrying
// it after more groups have arrived must not reuse that object's key: the retry
// covers a wider delta range, and a positional key would make Create return
// ErrExist, publish a root summary describing groups the object does not hold,
// and then reclaim the deltas that were their only other copy.
func TestWriter_Seal_RetryAfterFailedRootUpdate(t *testing.T) {
	objects := &FakeStore{
		SwapErrOnce: map[string]error{epochLogKey(testTrack, 1): errors.New("transient failure")},
	}

	w := newWriter(t, objects, Config{})

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.Error(t, w.Seal(t.Context()), "the first seal fails its log-root update")

	_, err := w.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)
	require.NoError(t, w.Seal(t.Context()))

	root := w.logRootCopy()
	require.Len(t, root.Sealed, 1)

	data, _, err := objects.Get(t.Context(), root.Sealed[0].Key)
	require.NoError(t, err)
	sealed, err := decodeManifest(data, func(m sealedManifest) int { return m.Version })
	require.NoError(t, err)

	assert.Equal(t, root.Sealed[0].Groups, len(sealed.Groups),
		"the root summary must match the sealed manifest it points at")

	r := openReader(t, objects)
	r.SeekStart()

	assert.Equal(t, []uint64{0, 1, 2, 3}, drain(t, r), "no committed group may become unreachable")
}

func TestWriter_Seal_Empty(t *testing.T) {
	w, _ := newTestWriter(t)

	require.NoError(t, w.Seal(t.Context()))
	assert.Empty(t, w.logRootCopy().Sealed, "sealing an empty open region must be a no-op")
}

func TestWriter_AppendGroup_SealsAtThreshold(t *testing.T) {
	// One byte guarantees every append crosses the threshold.
	w := newWriter(t, memstore.New(), Config{SealThreshold: 1})

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	assert.Len(t, w.logRootCopy().Sealed, 1, "crossing the threshold rotates the open region")
	assert.Equal(t, uint64(1), w.logRootCopy().OpenFrom)
}

func TestTrack_Writer(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(2) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	// Reopening stands in for a process restart.
	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	reopened, err := track.Writer(t.Context())
	require.NoError(t, err)

	assert.Equal(t, uint64(2), reopened.nextDelta)
	assert.Len(t, reopened.openGroups, 2)
	assert.Equal(t, uint32(90000), reopened.Root().Timescale)

	_, err = reopened.AppendGroup(t.Context(), testGroup(t, 2), []byte("payload"))
	require.NoError(t, err)

	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, 1, 2))
	assert.NoError(t, err, "the reopened writer must continue the delta sequence, not restart it")
}

// head is a cache. Losing it must cost nothing but a probe.
func TestTrack_Writer_WithoutHead(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	require.NoError(t, objects.Delete(t.Context(), headKey(testTrack, 1)))

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	reopened, err := track.Writer(t.Context())
	require.NoError(t, err)

	assert.Equal(t, uint64(3), reopened.nextDelta,
		"the true tip is found by probing, so a missing head loses nothing")
}

// A stale head must not stop recovery short either: probing continues past it.
func TestTrack_Writer_StaleHead(t *testing.T) {
	w, objects := newTestWriter(t)

	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}

	// Rewind head to point at the very first delta.
	stale, err := encodeManifest(head{Version: manifestVersion, Delta: 0})
	require.NoError(t, err)

	_, currentVersion, err := objects.Get(t.Context(), headKey(testTrack, 1))
	require.NoError(t, err)

	_, err = objects.Swap(t.Context(), headKey(testTrack, 1), stale, currentVersion)
	require.NoError(t, err)

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	reopened, err := track.Writer(t.Context())
	require.NoError(t, err)

	assert.Equal(t, uint64(3), reopened.nextDelta)
}

func TestOpen_TrackNotFound(t *testing.T) {
	_, err := Open(t.Context(), memstore.New(), testTrack, Config{})

	assert.ErrorIs(t, err, ErrTrackNotFound)
}

// The commit is the delta write, so a failure to publish head must leave the
// group committed and the call successful.
func TestWriter_AppendGroup_HeadFailureDoesNotFailCommit(t *testing.T) {
	headFailure := errors.New("head unavailable")
	objects := &FakeStore{SwapErr: map[string]error{headKey(testTrack, 1): headFailure}}

	w := newWriter(t, objects, Config{})

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err, "head is a discovery cache; failing to publish it must not fail a durable commit")

	_, _, err = objects.Get(t.Context(), deltaKey(testTrack, 1, 0))
	assert.NoError(t, err)
	_, _, err = objects.Get(t.Context(), meta.ObjectKey)
	assert.NoError(t, err)
}

// publishHead surfaces its error rather than handling it, which is what
// makes the swallow in AppendGroup an explicit decision instead of a hidden one.
// Called directly here without the lock, which is safe in a single-goroutine
// test.
func TestWriter_publishHead(t *testing.T) {
	headFailure := errors.New("head unavailable")
	objects := &FakeStore{SwapErr: map[string]error{headKey(testTrack, 1): headFailure}}

	w := newWriter(t, objects, Config{})

	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	err = w.publishHead(t.Context(), NewGroupID(1, 0))

	assert.ErrorIs(t, err, headFailure)
}

// The payload is written before the manifest, so a crash in between leaves an
// orphaned object that no reader can see — recoverable. The reverse order would
// leave a manifest pointing at nothing.
func TestWriter_AppendGroup_CommitOrder(t *testing.T) {
	objects := &FakeStore{}

	w := newWriter(t, objects, Config{})

	meta, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	_, creates, _, _ := objects.Calls()
	payloadIndex := indexOf(creates, meta.ObjectKey)
	deltaIndex := indexOf(creates, deltaKey(testTrack, 1, 0))

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
