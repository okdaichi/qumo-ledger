package ledger

import (
	"io"
	"iter"
	"math"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failureFixture is a track laid out so that every kind of object a Reader
// fetches exists and can be failed on its own. Epoch 1 holds groups 0-5, the
// first three sealed and the rest in the open region; epoch 2 holds two more,
// stamped later on the wall clock than anything in epoch 1.
type failureFixture struct {
	objects *fakeStore

	root   string // the track root
	log1   string // epoch 1's log
	log2   string // epoch 2's log
	head1  string // epoch 1's head pointer
	head2  string // epoch 2's head pointer
	sealed string // epoch 1's sealed manifest, groups 0-2
	open1  string // epoch 1's first open delta, group 3
	open2  string // epoch 2's first delta
	group  string // group 0's payload object

	// lateWallclock is where epoch 2's groups start on the wall clock.
	lateWallclock int64
}

func newFailureFixture(tb testing.TB) failureFixture {
	tb.Helper()
	ctx := tb.Context()

	objects := &fakeStore{}
	w := newWriter(tb, objects, Config{})

	var first GroupInfo
	for sequence := range uint64(6) {
		if sequence == 3 {
			require.NoError(tb, w.Seal(ctx))
		}
		group, err := w.AppendGroup(ctx, testGroup(tb, sequence), []byte("payload"))
		require.NoError(tb, err)
		if sequence == 0 {
			first = group
		}
	}

	lateWallclock := int64(wallclockBase + 100*nanosPerGroup)
	require.NoError(tb, w.NewEpoch(ctx))
	for sequence := range uint64(2) {
		group := testGroup(tb, sequence)
		group.Wallclock = lateWallclock + int64(sequence)*nanosPerGroup
		_, err := w.AppendGroup(ctx, group, []byte("payload"))
		require.NoError(tb, err)
	}

	logRoot, _, err := fetchEpochLog(ctx, objects, testTrack, 1)
	require.NoError(tb, err)
	require.Len(tb, logRoot.Sealed, 1, "the fixture seals exactly one run")

	return failureFixture{
		objects:       objects,
		root:          rootKey(testTrack),
		log1:          epochLogKey(testTrack, 1),
		log2:          epochLogKey(testTrack, 2),
		head1:         headKey(testTrack, 1),
		head2:         headKey(testTrack, 2),
		sealed:        logRoot.Sealed[0].Key,
		open1:         deltaKey(testTrack, 1, logRoot.OpenFrom),
		open2:         deltaKey(testTrack, 2, 0),
		group:         first.ObjectKey,
		lateWallclock: lateWallclock,
	}
}

// arm makes every Get of key fail from now on.
func (f failureFixture) arm(key string) {
	if f.objects.getErr == nil {
		f.objects.getErr = map[string]error{}
	}
	f.objects.getErr[key] = errStoreDown
}

// readAll drives Next until it stops, returning what stopped it: io.EOF when
// the walk reached the tip, anything else when it failed on the way.
func readAll(tb testing.TB, r *Reader) error {
	tb.Helper()
	for {
		if _, err := r.Next(tb.Context()); err != nil {
			return err
		}
	}
}

// rangeAll consumes a range, returning the first error it yields.
func rangeAll(tb testing.TB, seq iter.Seq2[GroupInfo, error]) error {
	tb.Helper()
	for _, err := range seq {
		if err != nil {
			return err
		}
	}
	return nil
}

// assertStoreFailure checks that op reports an outage as an outage. A Reader
// answers from several kinds of object — the track root, an epoch's log and
// head, its sealed manifests, its open deltas, the payloads. Whichever one
// cannot be fetched, the operation that needed it must fail with the store's
// error. Reporting the object as absent instead would turn an outage into "no
// such group", and a player or follower would act on that.
//
// op runs twice against a fresh Reader over the fixture: first with the store
// answering, so that the failure which follows is the armed key and nothing
// about the fixture, and then with key failing.
func assertStoreFailure(
	tb testing.TB,
	key func(failureFixture) string,
	op func(testing.TB, failureFixture, *Reader) error,
) {
	tb.Helper()

	fix := newFailureFixture(tb)
	if err := op(tb, fix, openReader(tb, fix.objects)); err != nil {
		require.ErrorIs(tb, err, io.EOF, "only reaching the tip may stop the operation")
	}

	r := openReader(tb, fix.objects)
	fix.arm(key(fix))

	err := op(tb, fix, r)
	require.Error(tb, err)
	assert.ErrorIs(tb, err, errStoreDown)
	assert.NotErrorIs(tb, err, ErrGroupNotFound, "an outage is not an absence")
	assert.NotErrorIs(tb, err, ErrEpochNotFound, "an outage is not an absence")
}

func TestReader_Refresh_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"track root": {key: func(f failureFixture) string { return f.root }},
		"epoch log":  {key: func(f failureFixture) string { return f.log1 }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.Refresh(tb.Context())
			})
		})
	}
}

func TestReader_Next_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"sealed manifest":           {key: func(f failureFixture) string { return f.sealed }},
		"open delta":                {key: func(f failureFixture) string { return f.open1 }},
		"the following epoch's log": {key: func(f failureFixture) string { return f.log2 }},
		// On reaching the tip a Reader re-reads the root, to learn of an epoch
		// begun since it opened. Failing that, it cannot say the track ended.
		"track root at the tip": {key: func(f failureFixture) string { return f.root }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				return readAll(tb, r)
			})
		})
	}
}

func TestReader_Lookup_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
		id  GroupID
	}{
		"epoch log":       {key: func(f failureFixture) string { return f.log2 }, id: NewGroupID(2, 0)},
		"open delta":      {key: func(f failureFixture) string { return f.open1 }, id: NewGroupID(1, 3)},
		"sealed manifest": {key: func(f failureFixture) string { return f.sealed }, id: NewGroupID(1, 1)},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Lookup(tb.Context(), tt.id)
				return err
			})
		})
	}
}

func TestReader_SeekTip_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"epoch log": {key: func(f failureFixture) string { return f.log2 }},
		"head":      {key: func(f failureFixture) string { return f.head2 }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.SeekTip(tb.Context())
			})
		})
	}
}

func TestReader_SeekAfter_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
		id  GroupID
	}{
		"epoch log":       {key: func(f failureFixture) string { return f.log2 }, id: NewGroupID(2, 0)},
		"sealed manifest": {key: func(f failureFixture) string { return f.sealed }, id: NewGroupID(1, 0)},
		"open delta":      {key: func(f failureFixture) string { return f.open1 }, id: NewGroupID(1, 2)},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.SeekAfter(tb.Context(), tt.id)
			})
		})
	}
}

func TestReader_SeekMedia_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"epoch log":  {key: func(f failureFixture) string { return f.log2 }},
		"open delta": {key: func(f failureFixture) string { return f.open2 }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.SeekMedia(tb.Context(), 0)
				return err
			})
		})
	}
}

// The target predates epoch 2 and every open group of epoch 1, so the seek has
// to reach into the sealed run to answer.
func TestReader_SeekWallclock_StoreFailure(t *testing.T) {
	assertStoreFailure(t,
		func(f failureFixture) string { return f.sealed },
		func(tb testing.TB, _ failureFixture, r *Reader) error {
			_, err := r.SeekWallclock(tb.Context(), wallclockBase+nanosPerGroup)
			return err
		},
	)
}

func TestReader_RangeMedia_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"epoch log":       {key: func(f failureFixture) string { return f.log2 }},
		"sealed manifest": {key: func(f failureFixture) string { return f.sealed }},
		"open delta":      {key: func(f failureFixture) string { return f.open1 }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				return rangeAll(tb, r.RangeMedia(tb.Context(), 0, math.MaxInt64))
			})
		})
	}
}

func TestReader_RangeWallclock_StoreFailure(t *testing.T) {
	assertStoreFailure(t,
		func(f failureFixture) string { return f.open2 },
		func(tb testing.TB, f failureFixture, r *Reader) error {
			return rangeAll(tb, r.RangeWallclock(tb.Context(), f.lateWallclock, math.MaxInt64))
		},
	)
}

func TestReader_Before_StoreFailure(t *testing.T) {
	tests := map[string]struct {
		key   func(failureFixture) string
		limit int
	}{
		"epoch log":  {key: func(f failureFixture) string { return f.log2 }, limit: 3},
		"head":       {key: func(f failureFixture) string { return f.head2 }, limit: 3},
		"open delta": {key: func(f failureFixture) string { return f.open2 }, limit: 3},
		// Asking for more than the open regions hold forces the sealed run.
		"sealed manifest": {key: func(f failureFixture) string { return f.sealed }, limit: 8},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assertStoreFailure(t, tt.key, func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Before(tb.Context(), 0, tt.limit)
				return err
			})
		})
	}
}

func TestReader_ReadGroup_StoreFailure(t *testing.T) {
	assertStoreFailure(t,
		func(f failureFixture) string { return f.group },
		func(tb testing.TB, f failureFixture, r *Reader) error {
			_, err := r.ReadGroup(tb.Context(), f.group)
			return err
		},
	)
}

func TestOpen_StoreFailure(t *testing.T) {
	fix := newFailureFixture(t)
	fix.arm(fix.root)

	_, err := Open(t.Context(), fix.objects, testTrack, Config{})

	assert.ErrorIs(t, err, errStoreDown)
	assert.NotErrorIs(t, err, ErrTrackNotFound, "an outage is not a missing track")
}

// A Reader starts at epoch 1, and cannot be handed out without that epoch's
// log.
func TestTrack_Reader_StoreFailure(t *testing.T) {
	fix := newFailureFixture(t)
	track, err := Open(t.Context(), fix.objects, testTrack, Config{})
	require.NoError(t, err)
	fix.arm(fix.log1)

	_, err = track.Reader(t.Context())

	assert.ErrorIs(t, err, errStoreDown)
}

func TestTrack_Reload_StoreFailure(t *testing.T) {
	fix := newFailureFixture(t)
	track, err := Open(t.Context(), fix.objects, testTrack, Config{})
	require.NoError(t, err)
	fix.arm(fix.root)

	assert.ErrorIs(t, track.Reload(t.Context()), errStoreDown)
	assert.Equal(t, uint64(2), track.LatestEpoch(), "a failed reload keeps what was loaded")
}

// overwrite replaces an object's bytes in place, standing in for a manifest
// that was damaged or written by something else. A manifest that cannot be
// trusted has to be refused where it is read: a truncated document would
// otherwise be taken for an empty one, and a log belonging to another track or
// epoch for this one's.
func overwrite(tb testing.TB, objects store.Store, key string, data []byte) {
	tb.Helper()

	_, version, err := objects.Get(tb.Context(), key)
	require.NoError(tb, err)
	_, err = objects.Swap(tb.Context(), key, data, version)
	require.NoError(tb, err)
}

func TestOpen_DamagedRoot(t *testing.T) {
	fix := newFailureFixture(t)
	overwrite(t, fix.objects, fix.root, []byte("{"))

	_, err := Open(t.Context(), fix.objects, testTrack, Config{})

	assert.Error(t, err)
}

func TestTrack_Reader_DamagedEpochLog(t *testing.T) {
	encode := func(root epochLogRoot) []byte {
		data, err := encodeManifest(root)
		require.NoError(t, err)
		return data
	}

	tests := map[string]struct {
		data    []byte
		wantErr error // nil when any error will do
	}{
		"not JSON": {
			data: []byte("{"),
		},
		"from a newer format": {
			data:    encode(epochLogRoot{Version: manifestVersion + 1, Track: testTrack, Epoch: 1}),
			wantErr: ErrUnsupportedVersion,
		},
		"of another track": {
			data:    encode(epochLogRoot{Version: manifestVersion, Track: "live/cam2/video", Epoch: 1}),
			wantErr: ErrManifestMismatch,
		},
		"of another epoch": {
			data:    encode(epochLogRoot{Version: manifestVersion, Track: testTrack, Epoch: 7}),
			wantErr: ErrManifestMismatch,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			track, err := Open(t.Context(), fix.objects, testTrack, Config{})
			require.NoError(t, err)
			overwrite(t, fix.objects, fix.log1, tt.data)

			_, err = track.Reader(t.Context())

			require.Error(t, err)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestReader_Next_DamagedSealedManifest(t *testing.T) {
	fix := newFailureFixture(t)
	overwrite(t, fix.objects, fix.sealed, []byte("{"))

	err := readAll(t, openReader(t, fix.objects))

	assert.NotErrorIs(t, err, io.EOF, "a damaged manifest is not the end of the track")
}

func TestReader_SeekTip_DamagedHead(t *testing.T) {
	fix := newFailureFixture(t)
	overwrite(t, fix.objects, fix.head2, []byte("{"))

	assert.Error(t, openReader(t, fix.objects).SeekTip(t.Context()))
}

// A range is an iterator, and a caller may stop it wherever it likes. Stopping
// inside the sealed history and inside the open region are separate loops, and
// neither may yield again once told to stop.
func TestReader_RangeMedia_StopsWhenToldTo(t *testing.T) {
	tests := map[string]struct {
		stopAfter int
	}{
		"inside the sealed run":  {stopAfter: 2},
		"inside the open region": {stopAfter: 5},
		"inside the next epoch":  {stopAfter: 7},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			r := openReader(t, fix.objects)

			var seen int
			for _, err := range r.RangeMedia(t.Context(), 0, math.MaxInt64) {
				require.NoError(t, err)
				seen++
				if seen == tt.stopAfter {
					break
				}
			}
			assert.Equal(t, tt.stopAfter, seen)
		})
	}
}

// An empty or inverted interval holds nothing, and says so without touching
// the store.
func TestReader_RangeWallclock_EmptyInterval(t *testing.T) {
	tests := map[string]struct {
		from, to int64
	}{
		"empty":    {from: wallclockBase, to: wallclockBase},
		"inverted": {from: wallclockBase + nanosPerGroup, to: wallclockBase},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			r := openReader(t, fix.objects)
			fix.objects.resetCalls()

			var yielded int
			for range r.RangeWallclock(t.Context(), tt.from, tt.to) {
				yielded++
			}
			assert.Zero(t, yielded, "an empty interval yields nothing")
			gets, _, _, _ := fix.objects.calls()
			assert.Empty(t, gets, "an empty interval needs no read")
		})
	}
}

// A position recorded in an epoch the track does not have — a state file from
// another deployment, say — resumes at the newest epoch there is rather than
// failing on a log that was never written.
func TestReader_SeekAfter_EpochBeyondTheLatest(t *testing.T) {
	fix := newFailureFixture(t)
	r := openReader(t, fix.objects)

	require.NoError(t, r.SeekAfter(t.Context(), NewGroupID(9, 0)))

	next, err := r.Next(t.Context())
	require.NoError(t, err)
	assert.Equal(t, NewGroupID(2, 1), next.ID, "resumes after sequence 0 of the newest epoch")
}
