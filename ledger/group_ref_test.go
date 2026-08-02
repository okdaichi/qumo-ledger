package ledger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGroupRef(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected GroupRef
		wantErr  bool
	}{
		"padded":          {input: "e000001-g00000042", expected: GroupRef{Epoch: 1, Sequence: 42}, wantErr: false},
		"unpadded":        {input: "e1-g42", expected: GroupRef{Epoch: 1, Sequence: 42}, wantErr: false},
		"empty is zero":   {input: "", expected: GroupRef{}, wantErr: false},
		"large epoch":     {input: "e000007-g00000000", expected: GroupRef{Epoch: 7, Sequence: 0}, wantErr: false},
		"missing prefix":  {input: "1-g42", wantErr: true},
		"missing divider": {input: "e1g42", wantErr: true},
		"no epoch":        {input: "e-g42", wantErr: true},
		"no sequence":     {input: "e1-g", wantErr: true},
		"non-numeric":     {input: "ex-gy", wantErr: true},
		"negative":        {input: "e-1-g1", wantErr: true},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ref, err := ParseGroupRef(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, ref)
		})
	}
}

// String and ParseGroupRef are inverses, so a persisted position round-trips.
func TestGroupRef_StringParseRoundTrip(t *testing.T) {
	refs := []GroupRef{
		{Epoch: 1, Sequence: 0},
		{Epoch: 1, Sequence: 42},
		{Epoch: 7, Sequence: 9999999},
	}
	for _, ref := range refs {
		parsed, err := ParseGroupRef(ref.String())
		require.NoError(t, err)
		assert.Equal(t, ref, parsed, "String/ParseGroupRef must round-trip %s", ref)
	}
}
