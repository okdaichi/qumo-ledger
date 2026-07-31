package ledger

import (
	"fmt"
	"strconv"
	"strings"
)

// Cursor marks a position in a track's commit stream: the point immediately
// after some group, or the start of the track for the zero value.
//
// It is deliberately opaque. What it holds is the ledger's internal commit
// numbering, and making that the public contract would freeze how commits are
// chunked — a caller would break the moment deltas were batched differently.
//
// Cursor is comparable, and implements encoding.TextMarshaler and
// encoding.TextUnmarshaler so a follower can persist its position and resume
// exactly where it stopped, including through JSON.
type Cursor struct {
	delta uint64
	index int
}

// String renders the cursor as "<delta>/<index>".
func (c Cursor) String() string {
	return strconv.FormatUint(c.delta, 10) + "/" + strconv.Itoa(c.index)
}

// MarshalText implements encoding.TextMarshaler.
func (c Cursor) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler. The empty string decodes
// to the zero Cursor, which is the start of the track.
func (c *Cursor) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		*c = Cursor{}
		return nil
	}

	deltaText, indexText, ok := strings.Cut(string(text), "/")
	if !ok {
		return fmt.Errorf("ledger: malformed cursor %q", text)
	}

	delta, err := strconv.ParseUint(deltaText, 10, 64)
	if err != nil {
		return fmt.Errorf("ledger: malformed cursor %q: %w", text, err)
	}

	index, err := strconv.Atoi(indexText)
	if err != nil || index < 0 {
		return fmt.Errorf("ledger: malformed cursor %q", text)
	}

	*c = Cursor{delta: delta, index: index}

	return nil
}

// Update is a group observed by a follower, paired with the cursor that resumes
// immediately after it.
//
// GroupInfo is embedded, so a consumer reaches the group's fields directly and
// only reaches for Cursor when it needs to record progress.
type Update struct {
	GroupInfo

	// Cursor resumes a follower at the group after this one.
	Cursor Cursor
}
