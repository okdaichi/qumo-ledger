package ledger

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursor_String(t *testing.T) {
	assert.Equal(t, "0/0", Cursor{}.String(), "the zero cursor is the start of the track")
	assert.Equal(t, "42/3", Cursor{delta: 42, index: 3}.String())
}

func TestCursor_MarshalText(t *testing.T) {
	tests := map[string]Cursor{
		"zero":    {},
		"typical": {delta: 42, index: 3},
		"wide":    {delta: 1 << 40, index: 1000},
	}

	for name, cursor := range tests {
		t.Run(name, func(t *testing.T) {
			text, err := cursor.MarshalText()
			require.NoError(t, err)

			var restored Cursor
			require.NoError(t, restored.UnmarshalText(text))
			assert.Equal(t, cursor, restored)
		})
	}
}

// A cursor is worth having only if it survives a restart, and a follower is
// most likely to store it inside a JSON state file.
func TestCursor_JSONRoundTrip(t *testing.T) {
	type state struct {
		Position Cursor `json:"position"`
	}

	data, err := json.Marshal(state{Position: Cursor{delta: 7, index: 2}})
	require.NoError(t, err)
	assert.JSONEq(t, `{"position":"7/2"}`, string(data))

	var restored state
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Equal(t, Cursor{delta: 7, index: 2}, restored.Position)
}

func TestCursor_UnmarshalText_Invalid(t *testing.T) {
	tests := map[string]string{
		"no separator":     "42",
		"empty delta":      "/3",
		"empty index":      "42/",
		"negative index":   "42/-1",
		"negative delta":   "-1/0",
		"not a number":     "abc/def",
		"trailing garbage": "42/3x",
	}

	for name, text := range tests {
		t.Run(name, func(t *testing.T) {
			var cursor Cursor
			assert.Error(t, cursor.UnmarshalText([]byte(text)))
		})
	}
}

// An absent cursor in a config file should mean "from the beginning" rather
// than an error, so a first run needs no special case.
func TestCursor_UnmarshalText_Empty(t *testing.T) {
	cursor := Cursor{delta: 9, index: 9}

	require.NoError(t, cursor.UnmarshalText(nil))

	assert.Equal(t, Cursor{}, cursor)
}
