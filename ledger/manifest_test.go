package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/root.manifest", RootKey("live/cam1/video"))
}

func TestHeadKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/delta/head", HeadKey("live/cam1/video"))
}

func TestDeltaKey(t *testing.T) {
	tests := map[string]struct {
		n        uint64
		expected string
	}{
		"first": {n: 0, expected: "live/cam1/video/delta/open/00000000.manifest"},
		"tenth": {n: 10, expected: "live/cam1/video/delta/open/00000010.manifest"},
		// Keys must stay sortable as strings for as long as possible, but the
		// ledger never lists, so overflowing the pad is harmless.
		"beyond the zero padding": {n: 123456789, expected: "live/cam1/video/delta/open/123456789.manifest"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, DeltaKey("live/cam1/video", tt.n))
		})
	}
}

func TestSealedKey(t *testing.T) {
	assert.Equal(t, "live/cam1/video/delta/sealed-000001.manifest", SealedKey("live/cam1/video", 1))
}

func TestGroupKey(t *testing.T) {
	assert.Equal(t,
		"live/cam1/video/groups/e000001-g00000042",
		GroupKey("live/cam1/video", GroupRef{Epoch: 1, Sequence: 42}),
	)
}

func TestSealedManifest_summarize(t *testing.T) {
	manifest := SealedManifest{
		FirstDelta: 4,
		LastDelta:  6,
		Groups: []GroupMeta{
			{GroupRef: GroupRef{Epoch: 1, Sequence: 10}, T0: 100, T1: 200, W0: 1000, W1: 2000},
			{GroupRef: GroupRef{Epoch: 1, Sequence: 11}, T0: 200, T1: 300, W0: 2000, W1: 3000},
			{GroupRef: GroupRef{Epoch: 1, Sequence: 12}, T0: 300, T1: 400, W0: 3000, W1: 4000},
		},
	}

	ref := manifest.summarize("sealed-000001.manifest")

	assert.Equal(t, "sealed-000001.manifest", ref.Key)
	assert.Equal(t, uint64(4), ref.FirstDelta)
	assert.Equal(t, uint64(6), ref.LastDelta)
	assert.Equal(t, 3, ref.Groups)
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 10}, ref.First)
	assert.Equal(t, GroupRef{Epoch: 1, Sequence: 12}, ref.Last)
	assert.Equal(t, int64(100), ref.T0)
	assert.Equal(t, int64(400), ref.T1)
	assert.Equal(t, int64(1000), ref.W0)
	assert.Equal(t, int64(4000), ref.W1)
}

// Commit order is not time order: a producer may backfill, and an epoch change
// resets media time outright. The summary must widen to cover every group or a
// range search will step over data that is really there.
func TestSealedManifest_summarize_OutOfOrderGroups(t *testing.T) {
	manifest := SealedManifest{
		Groups: []GroupMeta{
			{GroupRef: GroupRef{Epoch: 1, Sequence: 10}, T0: 500, T1: 600, W0: 5000, W1: 6000},
			{GroupRef: GroupRef{Epoch: 1, Sequence: 11}, T0: 100, T1: 200, W0: 1000, W1: 2000},
			{GroupRef: GroupRef{Epoch: 1, Sequence: 12}, T0: 900, T1: 1000, W0: 9000, W1: 10000},
		},
	}

	ref := manifest.summarize("k")

	assert.Equal(t, int64(100), ref.T0, "the summary must reach the earliest group, not the first one committed")
	assert.Equal(t, int64(1000), ref.T1)
	assert.Equal(t, int64(1000), ref.W0)
	assert.Equal(t, int64(10000), ref.W1)
}

func TestSealedManifest_summarize_Empty(t *testing.T) {
	ref := SealedManifest{FirstDelta: 1, LastDelta: 1}.summarize("k")

	assert.Equal(t, "k", ref.Key)
	assert.Equal(t, 0, ref.Groups)
	assert.Equal(t, GroupRef{}, ref.First)
}

func TestSealedRef_Covers(t *testing.T) {
	ref := SealedRef{W0: 1000, W1: 2000}

	tests := map[string]struct {
		instant  int64
		expected bool
	}{
		"before":      {instant: 999, expected: false},
		"lower bound": {instant: 1000, expected: true},
		"inside":      {instant: 1500, expected: true},
		"upper bound": {instant: 2000, expected: false},
		"after":       {instant: 2001, expected: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, ref.Covers(tt.instant))
		})
	}
}

func TestDecodeManifest_RejectsNewerVersion(t *testing.T) {
	data, err := encodeManifest(RootManifest{Version: ManifestVersion + 1, Track: "live/cam1"})
	require.NoError(t, err)

	_, err = decodeManifest(data, func(m RootManifest) int { return m.Version })

	assert.ErrorIs(t, err, ErrUnsupportedVersion,
		"an old binary must refuse a newer track rather than misread it")
}

func TestDecodeManifest_RoundTrip(t *testing.T) {
	root := RootManifest{
		Version:    ManifestVersion,
		Track:      "live/cam1/video",
		Timescale:  90000,
		TimeSource: TimeSourceFrame,
		MIME:       "video/mp4",
		Encoding:   "fmp4",
		Epoch:      3,
		OpenFrom:   7,
		Sealed:     []SealedRef{{Key: "k", Groups: 2}},
	}

	data, err := encodeManifest(root)
	require.NoError(t, err)

	got, err := decodeManifest(data, func(m RootManifest) int { return m.Version })
	require.NoError(t, err)

	assert.Equal(t, root, got)
}
