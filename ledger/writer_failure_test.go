package ledger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errStoreDown is the failure the store answers with in these tests.
var errStoreDown = errors.New("store is down")

// A failed write must not stop the next: the writer may have left an
// uncommitted group object behind, or the store may have taken a commit whose
// answer was lost, and either way the track goes on.
func TestWriter_Append_AfterAFailedWrite(t *testing.T) {
	group0 := groupKey(testTrack, NewGroupID(1, 0))
	delta0 := deltaKey(testTrack, 1, 0)
	tests := map[string]struct {
		fail      func(*FakeStore)
		committed []uint64
	}{
		"the group object fails": {
			fail:      func(s *FakeStore) { s.CreateErrOnce = map[string]error{group0: errStoreDown} },
			committed: []uint64{0},
		},
		"the commit fails, leaving the group object": {
			fail:      func(s *FakeStore) { s.CreateErrOnce = map[string]error{delta0: errStoreDown} },
			committed: []uint64{1},
		},
		"the group object is taken but its answer is lost": {
			fail:      func(s *FakeStore) { s.CreatedErrOnce = map[string]error{group0: errStoreDown} },
			committed: []uint64{1},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			objects := &FakeStore{}
			w := newWriter(t, objects, Config{})
			tt.fail(objects)

			_, err := w.Append(t.Context(), ticksPerGroup, []byte("first"))
			require.ErrorIs(t, err, errStoreDown)
			group, err := w.Append(t.Context(), ticksPerGroup, []byte("second"))

			require.NoError(t, err, "the writer recovers rather than colliding with what the failure left")
			assert.Equal(t, tt.committed[len(tt.committed)-1], group.ID.Sequence())
			assert.Equal(t, tt.committed, drain(t, openReader(t, objects)))
		})
	}
}

func TestWriter_Append_CommitTakenButItsAnswerLost(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	objects.CreatedErrOnce = map[string]error{deltaKey(testTrack, 1, 0): errStoreDown}

	first, err := w.Append(t.Context(), ticksPerGroup, []byte("first"))
	require.NoError(t, err, "the commit is read back and found to be this one")
	second, err := w.Append(t.Context(), ticksPerGroup, []byte("second"))
	require.NoError(t, err)

	assert.Equal(t, uint64(0), first.ID.Sequence())
	assert.Equal(t, uint64(1), second.ID.Sequence())
	assert.Equal(t, []uint64{0, 1}, drain(t, openReader(t, objects)))
}

func TestWriter_Append_CommitFailsAndCannotBeReadBack(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	delta0 := deltaKey(testTrack, 1, 0)
	objects.CreatedErrOnce = map[string]error{delta0: errStoreDown}
	objects.GetErrOnce = map[string]error{delta0: errStoreDown}

	_, err := w.Append(t.Context(), ticksPerGroup, []byte("first"))
	require.ErrorIs(t, err, errStoreDown, "an unconfirmed commit is reported")
	second, err := w.Append(t.Context(), ticksPerGroup, []byte("second"))

	require.NoError(t, err)
	assert.Equal(t, uint64(1), second.ID.Sequence(), "the reload finds the commit the store took")
	assert.Equal(t, []uint64{0, 1}, drain(t, openReader(t, objects)))
}

func TestWriter_Append_FollowsAnotherWritersCommit(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	other, err := track.Writer(t.Context())
	require.NoError(t, err)
	_, err = other.Append(t.Context(), ticksPerGroup, []byte("other"))
	require.NoError(t, err)

	group, err := w.Append(t.Context(), ticksPerGroup, []byte("this"))

	require.NoError(t, err)
	assert.Equal(t, uint64(1), group.ID.Sequence(), "the committed sequence is followed, not stepped past")
	assert.Equal(t, []uint64{0, 1}, drain(t, openReader(t, objects)))
}

func TestWriter_AppendGroup_DuplicateOfTheLastIsNotStepped(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("first"))
	require.NoError(t, err)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("again"))

	assert.ErrorIs(t, err, ErrGroupExists)
	assert.False(t, isGroupObjectCollision(err), "a duplicate of the last group is not an uncommitted object")
}

func TestWriter_Append_AfterARestartPastAnUncommittedGroup(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	objects.CreateErrOnce = map[string]error{deltaKey(testTrack, 1, 0): errStoreDown}
	_, err := w.Append(t.Context(), ticksPerGroup, []byte("lost"))
	require.ErrorIs(t, err, errStoreDown)

	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	restarted, err := track.Writer(t.Context())
	require.NoError(t, err)
	group, err := restarted.Append(t.Context(), ticksPerGroup, []byte("after the restart"))

	require.NoError(t, err)
	assert.Equal(t, uint64(1), group.ID.Sequence(), "the uncommitted sequence is stepped past")
	assert.Equal(t, []uint64{1}, drain(t, openReader(t, objects)))
}

func TestWriter_AppendGroup_ExplicitSequenceStillCollides(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	objects.CreateErrOnce = map[string]error{deltaKey(testTrack, 1, 0): errStoreDown}
	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("lost"))
	require.ErrorIs(t, err, errStoreDown)

	_, err = w.AppendGroup(t.Context(), testGroup(t, 0), []byte("again"))

	assert.ErrorIs(t, err, ErrGroupExists, "a caller that chose the sequence decides what to do about it")
}

func TestWriter_Append_GivesUpAfterManyUncommittedGroups(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	for seq := range uint64(maxSkippedSequences + 1) {
		_, err := objects.Create(t.Context(), groupKey(testTrack, NewGroupID(1, seq)), []byte("left behind"))
		require.NoError(t, err)
	}

	_, err := w.Append(t.Context(), ticksPerGroup, []byte("payload"))

	assert.ErrorIs(t, err, ErrGroupExists)
}

func TestWriter_Append_AfterAFailedSeal(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{SealThreshold: 1})
	objects.SwapErrOnce = map[string]error{epochLogKey(testTrack, 1): errStoreDown}

	first, err := w.Append(t.Context(), ticksPerGroup, []byte("first"))
	require.ErrorIs(t, err, errStoreDown)
	require.NotEmpty(t, first.ObjectKey, "the group is committed though the seal failed")
	second, err := w.Append(t.Context(), ticksPerGroup, []byte("second"))

	require.NoError(t, err)
	assert.Equal(t, uint64(1), second.ID.Sequence())
	assert.Equal(t, []uint64{0, 1}, drain(t, openReader(t, objects)))
}
