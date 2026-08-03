package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/root.manifest", rootKey("live/cam1/video"))
}

func TestEpochLogKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/e000001/log.manifest", epochLogKey("live/cam1/video", 1))
	assert.Equal(t, "live/cam1/video/e000002/log.manifest", epochLogKey("live/cam1/video", 2))
}

func TestHeadKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/e000001/delta/head", headKey("live/cam1/video", 1))
}

func TestDeltaKey(t *testing.T) {
	tests := map[string]struct {
		n        uint64
		expected string
	}{
		"first": {n: 0, expected: "live/cam1/video/e000001/delta/open/00000000.manifest"},
		"tenth": {n: 10, expected: "live/cam1/video/e000001/delta/open/00000010.manifest"},
		// Keys must stay sortable as strings for as long as possible, but the
		// ledger never lists, so overflowing the pad is harmless.
		"beyond the zero padding": {n: 123456789, expected: "live/cam1/video/e000001/delta/open/123456789.manifest"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, deltaKey("live/cam1/video", 1, tt.n))
		})
	}
}

// The key names the delta range so that a seal retried after more groups have
// arrived cannot collide with the earlier, smaller manifest.
func TestSealedKey(t *testing.T) {
	assert.Equal(t,
		"live/cam1/video/e000001/delta/sealed-00000000-00000002.manifest",
		sealedKey("live/cam1/video", 1, 0, 2),
	)
	assert.NotEqual(t,
		sealedKey("live/cam1/video", 1, 0, 2),
		sealedKey("live/cam1/video", 1, 0, 3),
		"a wider range must not reuse the key of a narrower one",
	)
}

func TestGroupKey(t *testing.T) {
	assert.Equal(t,
		"live/cam1/video/e000001/groups/g00000042",
		groupKey("live/cam1/video", NewGroupID(1, 42)),
	)
}

func TestSealedManifest_summarize(t *testing.T) {
	manifest := sealedManifest{
		FirstDelta: 4,
		LastDelta:  6,
		Groups: []GroupInfo{
			{ID: NewGroupID(1, 10), MediaTime: 100, Duration: 100, Wallclock: 1000},
			{ID: NewGroupID(1, 11), MediaTime: 200, Duration: 100, Wallclock: 2000},
			{ID: NewGroupID(1, 12), MediaTime: 300, Duration: 100, Wallclock: 3000},
		},
	}

	ref := manifest.summarize("sealed-000001.manifest")

	assert.Equal(t, "sealed-000001.manifest", ref.Key)
	assert.Equal(t, uint64(4), ref.FirstDelta)
	assert.Equal(t, uint64(6), ref.LastDelta)
	assert.Equal(t, 3, ref.Groups)
	assert.Equal(t, NewGroupID(1, 10), ref.First)
	assert.Equal(t, NewGroupID(1, 12), ref.Last)
	assert.Equal(t, int64(100), ref.MediaStart)
	assert.Equal(t, int64(400), ref.MediaEnd)
	assert.Equal(t, int64(1000), ref.WallclockStart)
	assert.Equal(t, int64(3000), ref.WallclockEnd, "wallclock bounds span anchors, since groups carry no wallclock end")
}

// Wallclock is optional, so the bounds must cover only the groups that have an
// anchor — a missing one is not an anchor at the Unix epoch.
func TestSealedManifest_summarize_PartialWallclock(t *testing.T) {
	manifest := sealedManifest{
		Groups: []GroupInfo{
			{ID: NewGroupID(1, 10), MediaTime: 100, Duration: 100},
			{ID: NewGroupID(1, 11), MediaTime: 200, Duration: 100, Wallclock: 5000},
			{ID: NewGroupID(1, 12), MediaTime: 300, Duration: 100, Wallclock: 7000},
		},
	}

	ref := manifest.summarize("k")

	assert.Equal(t, int64(5000), ref.WallclockStart, "the group without an anchor must not drag the start to zero")
	assert.Equal(t, int64(7000), ref.WallclockEnd)
}

func TestSealedManifest_summarize_NoWallclock(t *testing.T) {
	manifest := sealedManifest{
		Groups: []GroupInfo{{ID: NewGroupID(1, 1), MediaTime: 100, Duration: 100}},
	}

	ref := manifest.summarize("k")

	assert.Zero(t, ref.WallclockStart)
	assert.Zero(t, ref.WallclockEnd)
	assert.Equal(t, int64(200), ref.MediaEnd, "the media bounds are unaffected")
}

func TestSealedManifest_summarize_Empty(t *testing.T) {
	ref := sealedManifest{FirstDelta: 1, LastDelta: 1}.summarize("k")

	assert.Equal(t, "k", ref.Key)
	assert.Equal(t, 0, ref.Groups)
	assert.Equal(t, GroupID(0), ref.First)
}

func TestDecodeManifest_RejectsNewerVersion(t *testing.T) {
	data, err := encodeManifest(trackRoot{Version: manifestVersion + 1, Track: "live/cam1"})
	require.NoError(t, err)

	_, err = decodeManifest(data, func(m trackRoot) int { return m.Version })

	assert.ErrorIs(t, err, ErrUnsupportedVersion,
		"an old binary must refuse a newer track rather than misread it")
}

func TestDecodeManifest_RoundTrip(t *testing.T) {
	root := trackRoot{
		Version:     manifestVersion,
		Track:       "live/cam1/video",
		Timescale:   90000,
		TimeSource:  TimeSourceFrame,
		MIME:        "video/mp4",
		Encoding:    "fmp4",
		LatestEpoch: 3,
	}

	data, err := encodeManifest(root)
	require.NoError(t, err)

	got, err := decodeManifest(data, func(m trackRoot) int { return m.Version })
	require.NoError(t, err)

	assert.Equal(t, root, got)
}
