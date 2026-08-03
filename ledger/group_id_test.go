package ledger

import (
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
