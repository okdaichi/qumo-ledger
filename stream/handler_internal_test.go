package stream

import (
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/stretchr/testify/assert"
)

func TestParseSegmentID(t *testing.T) {
	tests := map[string]struct {
		base     string
		ext      string
		expected ledger.GroupID
		ok       bool
	}{
		"padded":             {base: "e000001-g00000042.m4s", ext: ".m4s", expected: ledger.NewGroupID(1, 42), ok: true},
		"unpadded":           {base: "e1-g42.m4s", ext: ".m4s", expected: ledger.NewGroupID(1, 42), ok: true},
		"other extension":    {base: "e000002-g00000000.ts", ext: ".ts", expected: ledger.NewGroupID(2, 0), ok: true},
		"wrong extension":    {base: "e000001-g00000042.ts", ext: ".m4s", ok: false},
		"missing extension":  {base: "e000001-g00000042", ext: ".m4s", expected: ledger.NewGroupID(1, 42), ok: true},
		"malformed":          {base: "segment.m4s", ext: ".m4s", ok: false},
		"non-numeric fields": {base: "ex-gy.m4s", ext: ".m4s", ok: false},
		// The zero GroupID names no group, so neither spelling of it is a segment.
		"zero id":        {base: "e000000-g00000000.m4s", ext: ".m4s", ok: false},
		"extension only": {base: ".m4s", ext: ".m4s", ok: false},
		"empty":          {base: "", ext: ".m4s", ok: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			id, ok := parseSegmentID(tt.base, tt.ext)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.expected, id)
		})
	}
}
