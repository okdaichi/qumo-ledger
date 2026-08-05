package fmp4_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/okdaichi/qumo-ledger/fmp4"
)

// box builds an ISO-BMFF box: a 32-bit size, a 4-character type, then payload.
func box(typ string, payload ...[]byte) []byte {
	var body []byte
	for _, p := range payload {
		body = append(body, p...)
	}
	out := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(out[0:4], uint32(8+len(body)))
	copy(out[4:8], typ)
	return append(out, body...)
}

func u32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func u64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// mdhdV0 is a version-0 media header carrying timescale.
func mdhdV0(timescale uint32) []byte {
	return box("mdhd",
		[]byte{0, 0, 0, 0}, // version 0, flags
		u32(0), u32(0),     // creation, modification
		u32(timescale),
		u32(0), // duration
	)
}

// mdhdV1 is the 64-bit variant, where timescale sits 8 bytes further in.
func mdhdV1(timescale uint32) []byte {
	return box("mdhd",
		[]byte{1, 0, 0, 0}, // version 1, flags
		u64(0), u64(0),     // creation, modification
		u32(timescale),
		u64(0), // duration
	)
}

func initSegment(mdhd []byte) []byte {
	return append(
		box("ftyp", []byte("iso5")),
		box("moov", box("trak", box("mdia", mdhd)))...,
	)
}

func TestTimescale(t *testing.T) {
	tests := map[string]struct {
		init     []byte
		expected uint32
		wantErr  bool
	}{
		"version 0":      {init: initSegment(mdhdV0(57600)), expected: 57600},
		"version 1":      {init: initSegment(mdhdV1(90000)), expected: 90000},
		"no mdhd":        {init: box("moov", box("trak")), wantErr: true},
		"empty":          {init: nil, wantErr: true},
		"mdhd truncated": {init: initSegment(box("mdhd", []byte{0, 0, 0, 0})), wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := fmp4.Timescale(tt.init)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// trak builds a track with the given track_ID and media timescale.
func trak(trackID, timescale uint32) []byte {
	tkhd := box("tkhd",
		[]byte{0, 0, 0, 0}, // version 0, flags
		u32(0), u32(0),     // creation, modification
		u32(trackID),
	)
	return box("trak", tkhd, box("mdia", mdhdV0(timescale)))
}

// An init describing several tracks has no single timescale, and picking one
// would silently scale every duration the caller computes by the ratio between
// two tracks' clocks.
func TestTimescale_MultipleTracks(t *testing.T) {
	init := box("moov", trak(1, 57600), trak(2, 48000))

	_, err := fmp4.Timescale(init)
	assert.ErrorIs(t, err, fmp4.ErrAmbiguousTrack)
}

func TestTimescaleForTrack(t *testing.T) {
	init := box("moov", trak(1, 57600), trak(2, 48000))

	tests := map[string]struct {
		trackID  uint32
		expected uint32
		wantErr  error
	}{
		"first track":  {trackID: 1, expected: 57600},
		"second track": {trackID: 2, expected: 48000},
		"absent track": {trackID: 9, wantErr: fmp4.ErrNotFound},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := fmp4.TimescaleForTrack(init, tt.trackID)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

// sample_count arrives off the wire, so a run that claims more samples than it
// could hold is rejected rather than multiplied into a duration that would
// become an EXTINF and a place on the ledger's timeline.
func TestFragmentDuration_ImplausibleSampleCount(t *testing.T) {
	t.Run("more entries than the trun holds", func(t *testing.T) {
		// flags 0x0301: data-offset, sample-duration and sample-size present, so
		// each entry is 8 bytes — but only one entry's worth follows.
		trun := box("trun", []byte{0, 0, 0x03, 0x01}, u32(1000), u32(0), u32(100), u32(500))
		fragment := box("moof", box("traf", box("tfhd", []byte{0, 0, 0, 0}, u32(1)), trun))

		_, err := fmp4.FragmentDuration(fragment)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "1000 samples")
	})

	t.Run("more samples than the media data could hold", func(t *testing.T) {
		// No per-sample entries at all, so only the mdat contradicts the count:
		// a sample occupies at least one byte.
		tfhd := box("tfhd", []byte{0, 0, 0, 0x08}, u32(1), u32(1920))
		trun := box("trun", []byte{0, 0, 0, 0x01}, u32(1_000_000), u32(0))
		fragment := append(
			box("moof", box("traf", tfhd, trun)),
			box("mdat", make([]byte, 4096))...,
		)

		_, err := fmp4.FragmentDuration(fragment)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bytes of media data")
	})

	t.Run("a plausible count is accepted", func(t *testing.T) {
		tfhd := box("tfhd", []byte{0, 0, 0, 0x08}, u32(1), u32(1920))
		trun := box("trun", []byte{0, 0, 0, 0x01}, u32(30), u32(0))
		fragment := append(
			box("moof", box("traf", tfhd, trun)),
			box("mdat", make([]byte, 4096))...,
		)

		got, err := fmp4.FragmentDuration(fragment)
		require.NoError(t, err)
		assert.Equal(t, uint64(30*1920), got)
	})
}

// A fixed-framerate encoder states the duration once in the tfhd and omits it
// per sample, so the run's extent is the sample count times that default.
func TestFragmentDuration_TfhdDefault(t *testing.T) {
	const samples, perSample = 30, 1920

	fragment := box("moof",
		box("traf",
			// flags 0x000008: default-sample-duration present.
			box("tfhd", []byte{0, 0, 0, 0x08}, u32(1), u32(perSample)),
			// flags 0x000001: data-offset present, no per-sample durations.
			box("trun", []byte{0, 0, 0, 0x01}, u32(samples), u32(0)),
		),
	)

	got, err := fmp4.FragmentDuration(fragment)
	require.NoError(t, err)
	assert.Equal(t, uint64(samples*perSample), got)
}

// A variable-framerate run carries a duration per sample, and the extent is
// their sum — not the count times the first one.
func TestFragmentDuration_PerSample(t *testing.T) {
	durations := []uint32{1000, 1500, 900, 1100}
	var entries []byte
	for _, d := range durations {
		// sample_duration followed by sample_size, per the flags below.
		entries = append(entries, u32(d)...)
		entries = append(entries, u32(500)...)
	}

	fragment := box("moof",
		box("traf",
			box("tfhd", []byte{0, 0, 0, 0}, u32(1)),
			// flags 0x000301: data-offset, sample-duration, sample-size present.
			box("trun", []byte{0, 0, 0x03, 0x01}, u32(uint32(len(durations))), u32(0), entries),
		),
	)

	got, err := fmp4.FragmentDuration(fragment)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000+1500+900+1100), got)
}

// Per-sample durations win over a tfhd default that also happens to be present.
func TestFragmentDuration_PerSampleOverridesDefault(t *testing.T) {
	fragment := box("moof",
		box("traf",
			box("tfhd", []byte{0, 0, 0, 0x08}, u32(1), u32(9999)),
			box("trun", []byte{0, 0, 0x01, 0x01}, u32(2), u32(0), u32(100), u32(200)),
		),
	)

	got, err := fmp4.FragmentDuration(fragment)
	require.NoError(t, err)
	assert.Equal(t, uint64(300), got)
}

// With neither per-sample durations nor a tfhd default there is nothing to
// measure, and inventing a value would be indistinguishable downstream from a
// real one.
func TestFragmentDuration_NoDurationAvailable(t *testing.T) {
	fragment := box("moof",
		box("traf",
			box("tfhd", []byte{0, 0, 0, 0}, u32(1)),
			box("trun", []byte{0, 0, 0, 0x01}, u32(30), u32(0)),
		),
	)

	_, err := fmp4.FragmentDuration(fragment)
	assert.ErrorIs(t, err, fmp4.ErrNotFound)
}

func TestFragmentDuration_MissingBoxes(t *testing.T) {
	tests := map[string]struct {
		fragment []byte
	}{
		"no moof": {fragment: box("mdat", []byte("payload"))},
		"no traf": {fragment: box("moof", box("mfhd", u32(1)))},
		"no trun": {fragment: box("moof", box("traf", box("tfhd", []byte{0, 0, 0, 0}, u32(1))))},
		"empty":   {fragment: nil},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := fmp4.FragmentDuration(tt.fragment)
			assert.ErrorIs(t, err, fmp4.ErrNotFound)
		})
	}
}

// A box scan must not run past its buffer on a size field that cannot be right,
// which is how malformed input turns into a panic rather than an error.
func TestFragmentDuration_MalformedSize(t *testing.T) {
	fragment := box("moof", box("traf"))
	// Overwrite the moof size with one that overruns the buffer.
	binary.BigEndian.PutUint32(fragment[0:4], 1<<30)

	_, err := fmp4.FragmentDuration(fragment)
	assert.ErrorIs(t, err, fmp4.ErrNotFound)
}
