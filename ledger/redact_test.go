package ledger

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrack_Redact(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 3, 0)
	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	reader := openReader(t, objects)
	target, err := reader.Lookup(t.Context(), NewGroupID(1, 1))
	require.NoError(t, err)

	require.NoError(t, track.Redact(t.Context(), target))

	_, err = reader.ReadGroup(t.Context(), target.ObjectKey)
	assert.ErrorIs(t, err, ErrGroupRedacted)
	redacted, err := track.Redacted(t.Context(), target)
	require.NoError(t, err)
	assert.True(t, redacted)
	still, err := reader.Lookup(t.Context(), target.ID)
	require.NoError(t, err)
	assert.Equal(t, target, still, "the manifest row stays")
	other, err := reader.Lookup(t.Context(), NewGroupID(1, 2))
	require.NoError(t, err)
	_, err = reader.ReadGroup(t.Context(), other.ObjectKey)
	require.NoError(t, err, "other groups are untouched")
	assert.NoError(t, track.Redact(t.Context(), target), "redacting again is not an error")
}

func TestTrack_Redacted_NotRedacted(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 1, 0)
	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)
	target, err := openReader(t, objects).Lookup(t.Context(), NewGroupID(1, 0))
	require.NoError(t, err)

	redacted, err := track.Redacted(t.Context(), target)

	require.NoError(t, err)
	assert.False(t, redacted)
}

func TestTrack_Redact_RefusesAnotherTracksGroup(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 1, 0)
	track, err := Open(t.Context(), objects, testTrack, Config{})
	require.NoError(t, err)

	tests := map[string]string{
		"another track":  "other/e000001/groups/g00000000",
		"not a group":    string(testTrack) + "/e000001/log.manifest",
		"no object name": string(testTrack) + "/e000001/groups/",
	}
	for name, key := range tests {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, track.Redact(t.Context(), GroupInfo{ObjectKey: key}), ErrInvalidGroup)
		})
	}
}

// A payload missing without a marker is a lost object, reported as the store's
// ErrNotExist, never as a redaction.
func TestReader_ReadGroup_LostIsNotRedacted(t *testing.T) {
	objects, _ := newPopulatedTrack(t, 1, 0)
	reader := openReader(t, objects)
	target, err := reader.Lookup(t.Context(), NewGroupID(1, 0))
	require.NoError(t, err)
	require.NoError(t, objects.Delete(t.Context(), target.ObjectKey))

	_, err = reader.ReadGroup(t.Context(), target.ObjectKey)

	assert.ErrorIs(t, err, store.ErrNotExist)
	assert.NotErrorIs(t, err, ErrGroupRedacted)
}
