package ledger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGroupID_String(t *testing.T) {
	tests := map[string]struct {
		id       GroupID
		expected string
	}{
		"zero":            {id: 0, expected: "e000000-g00000000"},
		"first group":     {id: NewGroupID(1, 1), expected: "e000001-g00000001"},
		"after a restart": {id: NewGroupID(2, 1), expected: "e000002-g00000001"},
		"wide sequence":   {id: NewGroupID(1, 123456789), expected: "e000001-g123456789"},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.id.String())
		})
	}
}

func TestGroupID_Before(t *testing.T) {
	tests := map[string]struct {
		left, right GroupID
		expected    bool
	}{
		"same epoch, ascending":  {left: NewGroupID(1, 1), right: NewGroupID(1, 2), expected: true},
		"same epoch, descending": {left: NewGroupID(1, 2), right: NewGroupID(1, 1), expected: false},
		"identical":              {left: NewGroupID(1, 1), right: NewGroupID(1, 1), expected: false},
		// A restart resets the producer's numbering, so a later epoch is later
		// in time even though its sequence is lower.
		"later epoch with a lower sequence":    {left: NewGroupID(2, 1), right: NewGroupID(1, 900), expected: false},
		"earlier epoch with a higher sequence": {left: NewGroupID(1, 900), right: NewGroupID(2, 1), expected: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.left.Before(tt.right))
		})
	}
}

func TestGroupInfo_MediaEnd(t *testing.T) {
	assert.Equal(t, int64(1500), GroupInfo{MediaTime: 500, Duration: 1000}.mediaEnd())
	assert.Equal(t, int64(500), GroupInfo{MediaTime: 500}.mediaEnd(),
		"with no duration the end collapses onto the anchor")
}

// Duration and Wallclock are optional, so every consumer needs a way to tell "absent"
// from a real value before using one.
func TestGroupInfo_HasDuration(t *testing.T) {
	assert.True(t, GroupInfo{Duration: 1}.hasDuration())
	assert.False(t, GroupInfo{}.hasDuration(), "zero means the producer did not supply an extent")
}

func TestGroupInfo_HasWallclock(t *testing.T) {
	assert.True(t, GroupInfo{Wallclock: 1}.hasWallclock())
	assert.False(t, GroupInfo{}.hasWallclock(), "zero means no anchor, not the Unix epoch")
}

func TestGroupInfo_wallclockEnd(t *testing.T) {
	tests := map[string]struct {
		group     GroupInfo
		timescale uint32
		expected  int64
		ok        bool
	}{
		"anchor and extent present": {
			// Two seconds at 90 kHz is 180000 units.
			group:     GroupInfo{Wallclock: 1_000_000_000, Duration: 180000},
			timescale: 90000,
			expected:  3_000_000_000,
			ok:        true,
		},
		"no wallclock anchor": {
			group:     GroupInfo{Duration: 180000},
			timescale: 90000,
			ok:        false,
		},
		"no duration": {
			group:     GroupInfo{Wallclock: 1_000_000_000},
			timescale: 90000,
			ok:        false,
		},
		"no timescale": {
			group:     GroupInfo{Wallclock: 1_000_000_000, Duration: 180000},
			timescale: 0,
			ok:        false,
		},
		// A coarse timescale reaches the int64 nanosecond ceiling with a
		// duration that is otherwise unremarkable.
		"conversion overflows": {
			group:     GroupInfo{Wallclock: 1, Duration: math.MaxInt64 / 2},
			timescale: 1,
			ok:        false,
		},
		"sum overflows the anchor": {
			group:     GroupInfo{Wallclock: math.MaxInt64 - 1, Duration: 90000},
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

func TestGroupInfo_validate(t *testing.T) {
	tests := map[string]struct {
		meta    GroupInfo
		wantErr bool
	}{
		"well formed": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), MediaTime: 0, Duration: 100, Wallclock: 1},
			wantErr: false,
		},
		"duration and wallclock are both optional": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), MediaTime: 100},
			wantErr: false,
		},
		"sequence zero is legal because producers may start there": {
			meta:    GroupInfo{ID: NewGroupID(1, 0), MediaTime: 0, Duration: 1},
			wantErr: false,
		},
		"epoch zero is reserved": {
			meta:    GroupInfo{ID: NewGroupID(0, 1)},
			wantErr: true,
		},
		"negative duration": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), MediaTime: 100, Duration: -1},
			wantErr: true,
		},
		"negative wallclock": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), Wallclock: -1},
			wantErr: true,
		},
		// A wrapped media end reads as before its own start, which would let
		// the ordering check wave through everything after it.
		"media range overflows": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), MediaTime: math.MaxInt64 - 1, Duration: 2},
			wantErr: true,
		},
		"negative size": {
			meta:    GroupInfo{ID: NewGroupID(1, 1), Size: -1},
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
