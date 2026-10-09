package ledger

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGroupID(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected GroupID
		wantErr  bool
	}{
		"padded":          {input: "e000001-g00000042", expected: NewGroupID(1, 42), wantErr: false},
		"unpadded":        {input: "e1-g42", expected: NewGroupID(1, 42), wantErr: false},
		"empty is zero":   {input: "", expected: 0, wantErr: false},
		"large epoch":     {input: "e000007-g00000000", expected: NewGroupID(7, 0), wantErr: false},
		"missing prefix":  {input: "1-g42", wantErr: true},
		"missing divider": {input: "e1g42", wantErr: true},
		"no epoch":        {input: "e-g42", wantErr: true},
		"no sequence":     {input: "e1-g", wantErr: true},
		"non-numeric":     {input: "ex-gy", wantErr: true},
		"negative":        {input: "e-1-g1", wantErr: true},
		"max sequence":    {input: "e1-g1099511627775", expected: NewGroupID(1, groupSeqMask), wantErr: false},
		// One past the 40 sequence bits would spill into the epoch.
		"sequence overflow": {input: "e1-g1099511627776", wantErr: true},
		"trailing garbage":  {input: "e1-g42.m4s", wantErr: true},
		"max epoch":         {input: "e16777215-g1", expected: NewGroupID(maxGroupEpoch, 1), wantErr: false},
		// One past the 24 epoch bits would be shifted out, leaving epoch 0.
		"epoch overflow":          {input: "e16777216-g1", wantErr: true},
		"epoch beyond 64 bits":    {input: "e18446744073709551616-g1", wantErr: true},
		"both parts at their max": {input: "e16777215-g1099511627775", expected: GroupID(math.MaxUint64), wantErr: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id, err := ParseGroupID(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, id)
		})
	}
}

// The two parts share one number, so a part too wide for its field must not
// change the other: a sequence that reached the epoch bits would move the group
// to a different lifetime and so to a different place in the track's order.
func TestNewGroupID(t *testing.T) {
	tests := map[string]struct {
		epoch, sequence    uint64
		wantEpoch, wantSeq uint64
	}{
		"zero":                  {epoch: 0, sequence: 0, wantEpoch: 0, wantSeq: 0},
		"typical":               {epoch: 3, sequence: 42, wantEpoch: 3, wantSeq: 42},
		"max of both":           {epoch: maxGroupEpoch, sequence: groupSeqMask, wantEpoch: maxGroupEpoch, wantSeq: groupSeqMask},
		"sequence one too wide": {epoch: 1, sequence: groupSeqMask + 1, wantEpoch: 1, wantSeq: 0},
		"sequence all ones":     {epoch: 1, sequence: math.MaxUint64, wantEpoch: 1, wantSeq: groupSeqMask},
		"epoch one too wide":    {epoch: maxGroupEpoch + 1, sequence: 7, wantEpoch: 0, wantSeq: 7},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id := NewGroupID(tt.epoch, tt.sequence)
			assert.Equal(t, tt.wantEpoch, id.Epoch())
			assert.Equal(t, tt.wantSeq, id.Sequence())
		})
	}
}

// String and ParseGroupID are inverses, so a persisted position round-trips.
func TestGroupID_StringParseRoundTrip(t *testing.T) {
	ids := []GroupID{
		NewGroupID(1, 0),
		NewGroupID(1, 42),
		NewGroupID(7, 9999999),
	}
	for _, id := range ids {
		parsed, err := ParseGroupID(id.String())
		require.NoError(t, err)
		assert.Equal(t, id, parsed, "String/ParseGroupID must round-trip %s", id)
	}
}
