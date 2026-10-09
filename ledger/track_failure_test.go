package ledger

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create writes the root and then epoch 1's log. A store that refuses the root
// has created nothing, and that is not the same as the track already existing.
func TestCreate_RootWriteFails(t *testing.T) {
	objects := &fakeStore{createErr: map[string]error{rootKey(testTrack): errStoreDown}}

	_, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})

	require.ErrorIs(t, err, errStoreDown)
	assert.NotErrorIs(t, err, ErrTrackExists)
}

// The root is durable before the first epoch's log is attempted, so a log that
// fails to write leaves a track that exists. The first Writer finishes the
// creation instead of the track being stuck half-made.
func TestCreate_EpochLogWriteFails(t *testing.T) {
	logKey := epochLogKey(testTrack, 1)
	objects := &fakeStore{createErr: map[string]error{logKey: errStoreDown}}

	_, err := Create(t.Context(), objects, testTrack, testSchema(t), Config{})
	require.ErrorIs(t, err, errStoreDown)

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err, "the root was written, so the track opens")

	// Still failing: the lazy creation reports the store's error.
	_, err = track.Writer(t.Context())
	require.ErrorIs(t, err, errStoreDown)

	// Once the store answers, the writer creates the log and is usable.
	delete(objects.createErr, logKey)
	w, err := track.Writer(t.Context())
	require.NoError(t, err)
	group, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)
	assert.Equal(t, NewGroupID(1, 0), group.ID)
}

func TestOpen_InvalidPath(t *testing.T) {
	_, err := Open(t.Context(), mem.New(), "", Config{})

	assert.ErrorIs(t, err, ErrInvalidTrackPath)
}

// A Writer is built from the epoch's log, its head and its open deltas. It must
// not be handed out on a guess about any of them: a writer that missed
// committed deltas would claim their numbers again.
func TestTrack_Writer_StoreFailures(t *testing.T) {
	tests := map[string]struct {
		key func(failureFixture) string
	}{
		"epoch log":  {key: func(f failureFixture) string { return f.log2 }},
		"head":       {key: func(f failureFixture) string { return f.head2 }},
		"open delta": {key: func(f failureFixture) string { return f.open2 }},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			fix := newFailureFixture(t)
			track, err := Open(t.Context(), fix.objects, testTrack, Config{})
			require.NoError(t, err)
			fix.arm(tt.key(fix))

			_, err = track.Writer(t.Context())

			assert.ErrorIs(t, err, errStoreDown)
		})
	}
}

func TestTrack_Writer_DamagedDelta(t *testing.T) {
	fix := newFailureFixture(t)
	overwrite(t, fix.objects, fix.open2, []byte("{"))
	track, err := Open(t.Context(), fix.objects, testTrack, Config{})
	require.NoError(t, err)

	_, err = track.Writer(t.Context())

	assert.Error(t, err)
}

// The head names a delta the store no longer has. Deltas are immutable, so this
// cannot be a writer catching up: something was lost, and resuming would
// silently truncate the epoch.
func TestTrack_Writer_DeltaMissingBelowHead(t *testing.T) {
	fix := newFailureFixture(t)
	require.NoError(t, fix.objects.Delete(t.Context(), fix.open2))
	track, err := Open(t.Context(), fix.objects, testTrack, Config{})
	require.NoError(t, err)

	_, err = track.Writer(t.Context())

	require.Error(t, err)
	assert.ErrorContains(t, err, "missing below head")
}

// A writer reopened right after a seal finds no open groups to learn the last
// committed one from. It takes the end of the newest sealed run instead, so the
// ordering rules still hold for its first append.
func TestTrack_Writer_ReopenedAfterSeal(t *testing.T) {
	objects := mem.New()
	w := newWriter(t, objects, Config{})
	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}
	require.NoError(t, w.Seal(t.Context()))

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	reopened, err := track.Writer(t.Context())
	require.NoError(t, err)

	// Group 1 lies inside what was sealed.
	_, err = reopened.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
	assert.Error(t, err, "a group overlapping the sealed run must be refused")

	next, err := reopened.AppendGroup(t.Context(), testGroup(t, 3), []byte("payload"))
	require.NoError(t, err)
	assert.Equal(t, NewGroupID(1, 3), next.ID)
}

// NewEpoch creates the next epoch's log, records it in the track root and reads
// the log back. A failure at any step leaves the writer in the epoch it had.
func TestWriter_NewEpoch_StoreFailures(t *testing.T) {
	nextLog := epochLogKey(testTrack, 2)
	tests := map[string]struct {
		objects *fakeStore
	}{
		"creating the log":   {objects: &fakeStore{createErr: map[string]error{nextLog: errStoreDown}}},
		"advancing the root": {objects: &fakeStore{swapErr: map[string]error{rootKey(testTrack): errStoreDown}}},
		"reading the log":    {objects: &fakeStore{getErr: map[string]error{nextLog: errStoreDown}}},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			w := newWriter(t, tt.objects, Config{})

			err := w.NewEpoch(t.Context())

			require.ErrorIs(t, err, errStoreDown)
			assert.Equal(t, uint64(1), w.Epoch(), "a failed NewEpoch does not advance the writer")
		})
	}
}

// The root moved under the writer but has not reached the new epoch: the bump
// is retried once against the root as it now stands.
func TestWriter_NewEpoch_RootChangedUnderneath(t *testing.T) {
	objects := &fakeStore{swapErrOnce: map[string]error{rootKey(testTrack): store.ErrVersionMismatch}}
	w := newWriter(t, objects, Config{})

	require.NoError(t, w.NewEpoch(t.Context()))

	assert.Equal(t, uint64(2), w.Epoch())
	reopened, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), reopened.LatestEpoch(), "the retried bump is what the store holds")
}

// Two handles on one track both begin epoch 2. The second finds the log already
// there and the root already advanced, and adopts both rather than failing or
// creating a third epoch.
func TestWriter_NewEpoch_AnotherWriterGotThereFirst(t *testing.T) {
	objects := mem.New()
	first := newWriter(t, objects, Config{})

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	second, err := track.Writer(t.Context())
	require.NoError(t, err)

	require.NoError(t, first.NewEpoch(t.Context()))
	require.NoError(t, second.NewEpoch(t.Context()))

	assert.Equal(t, uint64(2), first.Epoch())
	assert.Equal(t, uint64(2), second.Epoch())
	reopened, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), reopened.LatestEpoch())
}

// Two writers on one epoch race for a delta number. The loser's commit is
// refused by the store, which is what keeps one of them from overwriting the
// other's group row.
func TestWriter_AppendGroup_DeltaClaimedByAnotherWriter(t *testing.T) {
	objects := mem.New()
	first := newWriter(t, objects, Config{})

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	second, err := track.Writer(t.Context())
	require.NoError(t, err)

	_, err = first.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)

	// A different group, but the same delta number as far as second knows.
	_, err = second.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
	require.ErrorIs(t, err, store.ErrExist)

	// Having lost, second reloads the epoch and continues after first's group.
	next, err := second.AppendGroup(t.Context(), testGroup(t, 2), []byte("payload"))
	require.NoError(t, err)
	assert.Equal(t, NewGroupID(1, 2), next.ID)
	assert.Equal(t, []uint64{0, 2}, drain(t, openReader(t, objects)),
		"the refused group is not in the track; the others are, in order")
}

// After a commit whose outcome is unknown the writer reloads the epoch before
// its next append. If that reload cannot read the store, the append fails
// rather than proceeding on what the writer believed before.
func TestWriter_reloadIfStale_StoreFailures(t *testing.T) {
	appendGroup := func(tb testing.TB, w *Writer) error {
		_, err := w.AppendGroup(tb.Context(), testGroup(tb, 1), []byte("payload"))
		return err
	}
	appendNext := func(tb testing.TB, w *Writer) error {
		_, err := w.Append(tb.Context(), ticksPerGroup, []byte("payload"))
		return err
	}

	tests := map[string]struct {
		key string
		op  func(testing.TB, *Writer) error
	}{
		"AppendGroup, epoch log": {key: epochLogKey(testTrack, 1), op: appendGroup},
		"AppendGroup, head":      {key: headKey(testTrack, 1), op: appendGroup},
		"Append, epoch log":      {key: epochLogKey(testTrack, 1), op: appendNext},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			commit := deltaKey(testTrack, 1, 0)
			objects := &fakeStore{
				createErr: map[string]error{commit: errStoreDown},
				getErr:    map[string]error{},
			}
			w := newWriter(t, objects, Config{})

			// The commit fails and reading it back finds nothing, so the
			// writer no longer knows the epoch's state.
			_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
			require.ErrorIs(t, err, errStoreDown)

			delete(objects.createErr, commit)
			objects.getErr[tt.key] = errStoreDown

			err = tt.op(t, w)
			require.ErrorIs(t, err, errStoreDown)
			assert.ErrorContains(t, err, "after a failed write")

			// With the store answering again the same writer recovers.
			delete(objects.getErr, tt.key)
			require.NoError(t, tt.op(t, w))
		})
	}
}

// Sealing writes the sealed manifest before it touches the log root. If that
// write fails the root still points at the open deltas, so nothing is lost and
// the groups stay readable.
func TestWriter_Seal_ManifestWriteFails(t *testing.T) {
	objects := &fakeStore{createErr: map[string]error{}}
	w := newWriter(t, objects, Config{})
	for sequence := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, sequence), []byte("payload"))
		require.NoError(t, err)
	}
	objects.createErr[sealedKey(testTrack, 1, 0, 2)] = errStoreDown

	err := w.Seal(t.Context())

	require.ErrorIs(t, err, errStoreDown)
	assert.Equal(t, []uint64{0, 1, 2}, drain(t, openReader(t, objects)))
}

// The head is swapped against the version the writer last saw. When something
// else has moved it, or it has gone, the writer re-reads and tries once more;
// either way the head ends up naming the newest group.
func TestWriter_publishHead_Conflict(t *testing.T) {
	tests := map[string]struct {
		conflict error
		remove   bool
	}{
		"version moved": {conflict: store.ErrVersionMismatch},
		"head vanished": {conflict: store.ErrNotExist, remove: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			head := headKey(testTrack, 1)
			objects := &fakeStore{swapErrOnce: map[string]error{}}
			w := newWriter(t, objects, Config{})
			_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
			require.NoError(t, err)

			if tt.remove {
				require.NoError(t, objects.Delete(t.Context(), head))
			}
			objects.swapErrOnce[head] = tt.conflict

			_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
			require.NoError(t, err)

			h, _, err := fetchHead(t.Context(), objects, testTrack, 1)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), h.Delta, "the retry published the newest delta")
		})
	}
}
