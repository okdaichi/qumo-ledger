package ledger

import (
	"io"
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
	objects *FakeStore

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

	objects := &FakeStore{}
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
	if f.objects.GetErr == nil {
		f.objects.GetErr = map[string]error{}
	}
	f.objects.GetErr[key] = errStoreDown
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
func rangeAll(seq func(func(GroupInfo, error) bool)) error {
	for _, err := range seq {
		if err != nil {
			return err
		}
	}
	return nil
}

// A Reader answers from several kinds of object — the track root, an epoch's
// log and head, its sealed manifests, its open deltas, the payloads. Whichever
// one cannot be fetched, the operation that needed it must fail with the
// store's error. Reporting the object as absent instead would turn an outage
// into "no such group", and a player or follower would act on that.
func TestReader_StoreFailures(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
		op  func(testing.TB, failureFixture, *Reader) error
	}{
		"Refresh, track root": {
			key: func(f failureFixture) string { return f.root },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return r.Refresh(tb.Context()) },
		},
		"Refresh, epoch log": {
			key: func(f failureFixture) string { return f.log1 },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return r.Refresh(tb.Context()) },
		},
		"Next, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return readAll(tb, r) },
		},
		"Next, open delta": {
			key: func(f failureFixture) string { return f.open1 },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return readAll(tb, r) },
		},
		"Next, the following epoch's log": {
			key: func(f failureFixture) string { return f.log2 },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return readAll(tb, r) },
		},
		// On reaching the tip a Reader re-reads the root, to learn of an epoch
		// begun since it opened. Failing that, it cannot say the track ended.
		"Next at the tip, track root": {
			key: func(f failureFixture) string { return f.root },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return readAll(tb, r) },
		},
		"Lookup, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Lookup(tb.Context(), NewGroupID(2, 0))
				return err
			},
		},
		"Lookup, open delta": {
			key: func(f failureFixture) string { return f.open1 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Lookup(tb.Context(), NewGroupID(1, 3))
				return err
			},
		},
		"Lookup, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Lookup(tb.Context(), NewGroupID(1, 1))
				return err
			},
		},
		"SeekTip, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return r.SeekTip(tb.Context()) },
		},
		"SeekTip, head": {
			key: func(f failureFixture) string { return f.head2 },
			op:  func(tb testing.TB, _ failureFixture, r *Reader) error { return r.SeekTip(tb.Context()) },
		},
		"SeekAfter, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.SeekAfter(tb.Context(), NewGroupID(2, 0))
			},
		},
		"SeekAfter, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.SeekAfter(tb.Context(), NewGroupID(1, 0))
			},
		},
		"SeekAfter, open delta": {
			key: func(f failureFixture) string { return f.open1 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return r.SeekAfter(tb.Context(), NewGroupID(1, 2))
			},
		},
		"SeekMedia, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.SeekMedia(tb.Context(), 0)
				return err
			},
		},
		"SeekMedia, open delta": {
			key: func(f failureFixture) string { return f.open2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.SeekMedia(tb.Context(), 0)
				return err
			},
		},
		// The target predates epoch 2 and every open group of epoch 1, so the
		// seek has to reach into the sealed run to answer.
		"SeekWallclock, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.SeekWallclock(tb.Context(), wallclockBase+nanosPerGroup)
				return err
			},
		},
		"RangeMedia, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return rangeAll(r.RangeMedia(tb.Context(), 0, math.MaxInt64))
			},
		},
		"RangeMedia, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return rangeAll(r.RangeMedia(tb.Context(), 0, math.MaxInt64))
			},
		},
		"RangeMedia, open delta": {
			key: func(f failureFixture) string { return f.open1 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				return rangeAll(r.RangeMedia(tb.Context(), 0, math.MaxInt64))
			},
		},
		"RangeWallclock, open delta": {
			key: func(f failureFixture) string { return f.open2 },
			op: func(tb testing.TB, f failureFixture, r *Reader) error {
				return rangeAll(r.RangeWallclock(tb.Context(), f.lateWallclock, math.MaxInt64))
			},
		},
		"Before, epoch log": {
			key: func(f failureFixture) string { return f.log2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Before(tb.Context(), 0, 3)
				return err
			},
		},
		"Before, head": {
			key: func(f failureFixture) string { return f.head2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Before(tb.Context(), 0, 3)
				return err
			},
		},
		"Before, open delta": {
			key: func(f failureFixture) string { return f.open2 },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Before(tb.Context(), 0, 3)
				return err
			},
		},
		// Asking for more than the open regions hold forces the sealed run.
		"Before, sealed manifest": {
			key: func(f failureFixture) string { return f.sealed },
			op: func(tb testing.TB, _ failureFixture, r *Reader) error {
				_, err := r.Before(tb.Context(), 0, 8)
				return err
			},
		},
		"ReadGroup, payload": {
			key: func(f failureFixture) string { return f.group },
			op: func(tb testing.TB, f failureFixture, r *Reader) error {
				_, err := r.ReadGroup(tb.Context(), f.group)
				return err
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			r := openReader(t, fix.objects)

			// The same operation succeeds while the store answers, so the
			// failure below is the armed key and nothing about the fixture.
			if err := tt.op(t, fix, r); err != nil {
				require.ErrorIs(t, err, io.EOF, "only reaching the tip may stop the operation")
			}

			r = openReader(t, fix.objects)
			fix.arm(tt.key(fix))

			err := tt.op(t, fix, r)
			require.Error(t, err)
			assert.ErrorIs(t, err, errStoreDown)
			assert.NotErrorIs(t, err, ErrGroupNotFound, "an outage is not an absence")
			assert.NotErrorIs(t, err, ErrEpochNotFound, "an outage is not an absence")
		})
	}
}

// Opening needs the root and then epoch 1's log; a Reader cannot be handed out
// without either.
func TestTrack_Reader_StoreFailures(t *testing.T) {
	t.Run("track root", func(t *testing.T) {
		fix := newFailureFixture(t)
		fix.arm(fix.root)

		_, err := Open(t.Context(), fix.objects, testTrack, Config{})
		assert.ErrorIs(t, err, errStoreDown)
		assert.NotErrorIs(t, err, ErrTrackNotFound, "an outage is not a missing track")
	})

	t.Run("epoch log", func(t *testing.T) {
		fix := newFailureFixture(t)
		track, err := Open(t.Context(), fix.objects, testTrack, Config{})
		require.NoError(t, err)
		fix.arm(fix.log1)

		_, err = track.Reader(t.Context())
		assert.ErrorIs(t, err, errStoreDown)
	})

	t.Run("reload", func(t *testing.T) {
		fix := newFailureFixture(t)
		track, err := Open(t.Context(), fix.objects, testTrack, Config{})
		require.NoError(t, err)
		fix.arm(fix.root)

		assert.ErrorIs(t, track.Reload(t.Context()), errStoreDown)
		assert.Equal(t, uint64(2), track.LatestEpoch(), "a failed reload keeps what was loaded")
	})
}

// overwrite replaces an object's bytes in place, standing in for a manifest
// that was damaged or written by something else.
func overwrite(tb testing.TB, objects store.Store, key string, data []byte) {
	tb.Helper()

	_, version, err := objects.Get(tb.Context(), key)
	require.NoError(tb, err)
	_, err = objects.Swap(tb.Context(), key, data, version)
	require.NoError(tb, err)
}

// A manifest that cannot be trusted is refused where it is read. Each of these
// would otherwise be interpreted: a truncated document as an empty one, a log
// belonging to another track or epoch as this one's.
func TestReader_DamagedManifests(t *testing.T) {
	otherTrack, err := encodeManifest(epochLogRoot{Version: manifestVersion, Track: "live/cam2/video", Epoch: 1})
	require.NoError(t, err)
	otherEpoch, err := encodeManifest(epochLogRoot{Version: manifestVersion, Track: testTrack, Epoch: 7})
	require.NoError(t, err)
	newerLog, err := encodeManifest(epochLogRoot{Version: manifestVersion + 1, Track: testTrack, Epoch: 1})
	require.NoError(t, err)

	tests := map[string]struct {
		key     func(failureFixture) string
		data    []byte
		op      func(testing.TB, failureFixture) error
		wantErr error // nil when any error will do
	}{
		"track root is not JSON": {
			key:  func(f failureFixture) string { return f.root },
			data: []byte("{"),
			op: func(tb testing.TB, f failureFixture) error {
				_, err := Open(tb.Context(), f.objects, testTrack, Config{})
				return err
			},
		},
		"epoch log is not JSON": {
			key:  func(f failureFixture) string { return f.log1 },
			data: []byte("{"),
			op:   openReaderErr,
		},
		"epoch log from a newer format": {
			key:     func(f failureFixture) string { return f.log1 },
			data:    newerLog,
			op:      openReaderErr,
			wantErr: ErrUnsupportedVersion,
		},
		"epoch log of another track": {
			key:     func(f failureFixture) string { return f.log1 },
			data:    otherTrack,
			op:      openReaderErr,
			wantErr: ErrManifestMismatch,
		},
		"epoch log of another epoch": {
			key:     func(f failureFixture) string { return f.log1 },
			data:    otherEpoch,
			op:      openReaderErr,
			wantErr: ErrManifestMismatch,
		},
		"sealed manifest is not JSON": {
			key:  func(f failureFixture) string { return f.sealed },
			data: []byte("{"),
			op: func(tb testing.TB, f failureFixture) error {
				return readAll(tb, openReader(tb, f.objects))
			},
		},
		"head is not JSON": {
			key:  func(f failureFixture) string { return f.head2 },
			data: []byte("{"),
			op: func(tb testing.TB, f failureFixture) error {
				return openReader(tb, f.objects).SeekTip(tb.Context())
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			overwrite(t, fix.objects, tt.key(fix), tt.data)

			err := tt.op(t, fix)
			require.Error(t, err)
			assert.NotErrorIs(t, err, io.EOF)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

// openReaderErr opens the fixture's track and asks for a Reader, returning
// whichever step failed.
func openReaderErr(tb testing.TB, f failureFixture) error {
	tb.Helper()

	track, err := Open(tb.Context(), f.objects, testTrack, Config{})
	if err != nil {
		return err
	}
	_, err = track.Reader(tb.Context())
	return err
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
			fix.objects.ResetCalls()

			for range r.RangeWallclock(t.Context(), tt.from, tt.to) {
				t.Fatal("an empty interval yielded a group")
			}
			gets, _, _, _ := fix.objects.Calls()
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
