package ledger

import (
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

func TestGroupMeta_Contains(t *testing.T) {
	group := GroupMeta{T0: 1000, T1: 2000}

	tests := map[string]struct {
		mediaTime int64
		expected  bool
	}{
		"before":             {mediaTime: 999, expected: false},
		"at the lower bound": {mediaTime: 1000, expected: true},
		"inside":             {mediaTime: 1500, expected: true},
		"at the upper bound": {mediaTime: 2000, expected: false},
		"after":              {mediaTime: 2001, expected: false},
		"negative":           {mediaTime: -1, expected: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tt.expected, group.Contains(tt.mediaTime))
		})
	}
}

func TestGroupMeta_ContainsWallclock(t *testing.T) {
	group := GroupMeta{W0: 1_000_000, W1: 3_000_000}

	assert.False(t, group.ContainsWallclock(999_999))
	assert.True(t, group.ContainsWallclock(1_000_000), "the range is half-open, so the lower bound is included")
	assert.False(t, group.ContainsWallclock(3_000_000), "the range is half-open, so the upper bound is excluded")
}

func TestGroupMeta_Duration(t *testing.T) {
	assert.Equal(t, int64(1000), GroupMeta{T0: 500, T1: 1500}.Duration())
	assert.Equal(t, int64(0), GroupMeta{T0: 500, T1: 500}.Duration())
}

func TestGroupMeta_validate(t *testing.T) {
	tests := map[string]struct {
		meta    GroupMeta
		wantErr bool
	}{
		"well formed": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 0, T1: 100, W0: 1, W1: 2},
			wantErr: false,
		},
		"zero-length group is legal": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 100, T1: 100},
			wantErr: false,
		},
		"sequence zero is legal because producers may start there": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 0}, T0: 0, T1: 1},
			wantErr: false,
		},
		"epoch zero is reserved": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 0, Sequence: 1}},
			wantErr: true,
		},
		"inverted media range": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, T0: 100, T1: 0},
			wantErr: true,
		},
		"inverted wallclock range": {
			meta:    GroupMeta{GroupRef: GroupRef{Epoch: 1, Sequence: 1}, W0: 100, W1: 0},
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
