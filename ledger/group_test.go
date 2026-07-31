package ledger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupRef_String(t *testing.T) {
	tests := map[string]struct {
		ref      GroupRef
		expected string
	}{
		"zero":            {ref: GroupRef{}, expected: "e000000-g00000000"},
		"first group":     {ref: GroupRef{Epoch: 1, Sequence: 1}, expected: "e000001-g00000001"},
		"after a restart": {ref: GroupRef{Epoch: 2, Sequence: 1}, expected: "e000002-g00000001"},
		"wide sequence":   {ref: GroupRef{Epoch: 1, Sequence: 123456789}, expected: "e000001-g123456789"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.ref.String())
		})
	}
}

func TestGroupRef_Before(t *testing.T) {
	tests := map[string]struct {
		left     GroupRef
		right    GroupRef
		expected bool
	}{
		"same epoch, ascending":  {left: GroupRef{1, 1}, right: GroupRef{1, 2}, expected: true},
		"same epoch, descending": {left: GroupRef{1, 2}, right: GroupRef{1, 1}, expected: false},
		"identical":              {left: GroupRef{1, 1}, right: GroupRef{1, 1}, expected: false},
		// A restart resets the producer's numbering, so a later epoch is later
		// in time even though its sequence is lower.
		"later epoch with a lower sequence":    {left: GroupRef{2, 1}, right: GroupRef{1, 900}, expected: false},
		"earlier epoch with a higher sequence": {left: GroupRef{1, 900}, right: GroupRef{2, 1}, expected: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.left.Before(tt.right))
		})
	}
}

func TestGroupMeta_MediaEnd(t *testing.T) {
	assert.Equal(t, int64(1500), GroupMeta{T0: 500, Duration: 1000}.MediaEnd())
	assert.Equal(t, int64(500), GroupMeta{T0: 500}.MediaEnd(),
		"with no duration the end collapses onto the anchor")
}

// Duration and W0 are optional, so every consumer needs a way to tell "absent"
// from a real value before using one.
func TestGroupMeta_HasDuration(t *testing.T) {
	assert.True(t, GroupMeta{Duration: 1}.HasDuration())
	assert.False(t, GroupMeta{}.HasDuration(), "zero means the producer did not supply an extent")
}

func TestGroupMeta_HasWallclock(t *testing.T) {
	assert.True(t, GroupMeta{W0: 1}.HasWallclock())
	assert.False(t, GroupMeta{}.HasWallclock(), "zero means no anchor, not the Unix epoch")
}

func TestGroupMeta_wallclockEnd(t *testing.T) {
	tests := map[string]struct {
		group     GroupMeta
		timescale uint32
		expected  int64
		ok        bool
	}{
		"anchor and extent present": {
			// Two seconds at 90 kHz is 180000 units.
			group:     GroupMeta{W0: 1_000_000_000, Duration: 180000},
			timescale: 90000,
			expected:  3_000_000_000,
			ok:        true,
		},
		"no wallclock anchor": {
			group:     GroupMeta{Duration: 180000},
			timescale: 90000,
			ok:        false,
		},
		"no duration": {
			group:     GroupMeta{W0: 1_000_000_000},
			timescale: 90000,
			ok:        false,
		},
		"no timescale": {
			group:     GroupMeta{W0: 1_000_000_000, Duration: 180000},
			timescale: 0,
			ok:        false,
		},
		// A coarse timescale reaches the int64 nanosecond ceiling with a
		// duration that is otherwise unremarkable.
		"conversion overflows": {
			group:     GroupMeta{W0: 1, Duration: math.MaxInt64 / 2},
			timescale: 1,
			ok:        false,
		},
		"sum overflows the anchor": {
			group:     GroupMeta{W0: math.MaxInt64 - 1, Duration: 90000},
			timescale: 90000,
			ok:        false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, ok := tt.group.wallclockEnd(tt.timescale)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestGroupMeta_validate(t *testing.T) {
	tests := map[string]struct {
		meta    GroupMeta
		wantErr bool
	}{
		"well formed": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 0, Duration: 100, W0: 1},
			wantErr: false,
		},
		"duration and wallclock are both optional": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 100},
			wantErr: false,
		},
		"sequence zero is legal because producers may start there": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 0}, T0: 0, Duration: 1},
			wantErr: false,
		},
		"epoch zero is reserved": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 0, Sequence: 1}},
			wantErr: true,
		},
		"negative duration": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 100, Duration: -1},
			wantErr: true,
		},
		"negative wallclock": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, W0: -1},
			wantErr: true,
		},
		// A wrapped media end reads as before its own start, which would let
		// the ordering check wave through everything after it.
		"media range overflows": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: math.MaxInt64 - 1, Duration: 2},
			wantErr: true,
		},
		"negative size": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, Size: -1},
			wantErr: true,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			err := tt.meta.validate()
			if tt.wantErr {
				assert.ErrorIs(t, err, ErrInvalidGroup)
				return
			}
			assert.NoError(t, err)
		})
	}
}
