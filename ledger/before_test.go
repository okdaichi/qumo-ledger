package ledger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ids returns the GroupIDs of groups.
func ids(groups []GroupInfo) []GroupID {
	if groups == nil {
		return nil
	}
	out := make([]GroupID, len(groups))
	for i, g := range groups {
		out[i] = g.ID
	}
	return out
}

func TestReader_Before(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 5, 3) // sealed: 0,1,2  open: 3,4
	g := func(seq uint64) GroupID { return NewGroupID(1, seq) }

	tests := map[string]struct {
		before GroupID
		n      int
		want   []GroupID
	}{
		"the newest":                {before: 0, n: 2, want: []GroupID{g(3), g(4)}},
		"more than there are":       {before: 0, n: 10, want: []GroupID{g(0), g(1), g(2), g(3), g(4)}},
		"across sealed and open":    {before: g(4), n: 3, want: []GroupID{g(1), g(2), g(3)}},
		"within sealed history":     {before: g(2), n: 5, want: []GroupID{g(0), g(1)}},
		"exclusive of the named":    {before: g(3), n: 1, want: []GroupID{g(2)}},
		"nothing before the first":  {before: g(0), n: 5, want: nil},
		"after the tip":             {before: g(99), n: 3, want: []GroupID{g(2), g(3), g(4)}},
		"none asked for":            {before: 0, n: 0, want: nil},
		"a later epoch than exists": {before: NewGroupID(5, 0), n: 2, want: []GroupID{g(3), g(4)}},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r := openReader(t, objects)

			got, err := r.Before(t.Context(), tt.before, tt.n)

			require.NoError(t, err)
			assert.Equal(t, tt.want, ids(got))
		})
	}
}

func TestReader_Before_PagesBackThroughTheTrack(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 7, 4) // sealed: 0-3  open: 4-6
	r := openReader(t, objects)

	var pages [][]GroupID
	var cursor GroupID
	for {
		page, err := r.Before(t.Context(), cursor, 3)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		pages = append(pages, ids(page))
		cursor = page[0].ID
	}

	g := func(seq uint64) GroupID { return NewGroupID(1, seq) }
	assert.Equal(t, [][]GroupID{{g(4), g(5), g(6)}, {g(1), g(2), g(3)}, {g(0)}}, pages)
}

func TestReader_Before_AcrossEpochs(t *testing.T) {
	w, objects := newTestWriter(t)
	for seq := range uint64(3) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, seq), []byte("first lifetime"))
		require.NoError(t, err)
	}
	require.NoError(t, w.NewEpoch(t.Context()))
	for seq := range uint64(2) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, seq), []byte("second lifetime"))
		require.NoError(t, err)
	}
	r := openReader(t, objects)

	newest, err := r.Before(t.Context(), 0, 4)
	require.NoError(t, err)
	older, err := r.Before(t.Context(), NewGroupID(2, 0), 5)
	require.NoError(t, err)

	assert.Equal(t, []GroupID{NewGroupID(1, 1), NewGroupID(1, 2), NewGroupID(2, 0), NewGroupID(2, 1)}, ids(newest))
	assert.Equal(t, []GroupID{NewGroupID(1, 0), NewGroupID(1, 1), NewGroupID(1, 2)}, ids(older),
		"before the first group of an epoch is the end of the previous one")
}

func TestReader_Before_SealDuringTheRead(t *testing.T) {
	w, objects := newTestWriter(t)
	for seq := range uint64(5) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, seq), []byte("payload"))
		require.NoError(t, err)
	}
	r := openReader(t, &fakeSealingStore{Store: objects, writer: w})

	got, err := r.Before(t.Context(), 0, 10)

	require.NoError(t, err)
	assert.Equal(t, []GroupID{NewGroupID(1, 0), NewGroupID(1, 1), NewGroupID(1, 2), NewGroupID(1, 3), NewGroupID(1, 4)}, ids(got),
		"groups a seal moves out of the open region mid-read are still returned")
}

func TestReader_Before_HeadLaggingTheTip(t *testing.T) {
	// A head update that fails is dropped, leaving the head behind the
	// committed tip.
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	_, err := w.AppendGroup(t.Context(), testGroup(t, 0), []byte("payload"))
	require.NoError(t, err)
	objects.SwapErr = map[string]error{headKey(testTrack, 1): errors.New("head is down")}
	_, err = w.AppendGroup(t.Context(), testGroup(t, 1), []byte("payload"))
	require.NoError(t, err)

	got, err := openReader(t, objects).Before(t.Context(), 0, 5)

	require.NoError(t, err)
	assert.Equal(t, []GroupID{NewGroupID(1, 0), NewGroupID(1, 1)}, ids(got),
		"groups committed past the head are found")
}

func TestReader_Before_ReadsOnlyTheNewestDeltas(t *testing.T) {
	objects := &FakeStore{}
	w := newWriter(t, objects, Config{})
	for seq := range uint64(20) {
		_, err := w.AppendGroup(t.Context(), testGroup(t, seq), []byte("payload"))
		require.NoError(t, err)
	}
	r := openReader(t, objects)
	before, _, _, _ := objects.Calls()

	_, err := r.Before(t.Context(), 0, 3)
	require.NoError(t, err)

	after, _, _, _ := objects.Calls()
	assert.LessOrEqual(t, len(after)-len(before), 3+3,
		"a page of the newest three reads the log root, the head, one probe and three deltas")
}
