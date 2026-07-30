package ledger

import "fmt"

// GroupRef identifies a group within a track.
//
// Sequence alone is not an identity. Producers reset their numbering when they
// restart, and because group objects are immutable a reused sequence would
// collide rather than overwrite — leaving the track wedged. Epoch separates
// each producer lifetime into its own keyspace, so a restart continues cleanly
// while the producer's original sequence numbers survive intact for clients
// aligning replay against a live relay.
type GroupRef struct {
	// Epoch increments each time a producer restarts its numbering.
	Epoch uint64 `json:"epoch"`

	// Sequence is the producer's own group sequence. Gaps are expected:
	// groups are dropped under congestion, and the gap is real information.
	Sequence uint64 `json:"seq"`
}

// String returns a human-readable form matching the object key layout.
func (r GroupRef) String() string {
	return fmt.Sprintf("e%06d-g%08d", r.Epoch, r.Sequence)
}

// Before reports whether r orders before other. Epoch dominates, since a later
// epoch always describes later data even when its sequence numbers are lower.
func (r GroupRef) Before(other GroupRef) bool {
	if r.Epoch != other.Epoch {
		return r.Epoch < other.Epoch
	}

	return r.Sequence < other.Sequence
}

// GroupMeta is a group's manifest row: everything a reader needs to decide
// whether to fetch the payload, without fetching it.
//
// Both time ranges are half-open, [T0, T1) and [W0, W1).
type GroupMeta struct {
	GroupRef

	// T0 and T1 bound the group in media time, expressed in the track's
	// timescale units. This is the precise intra-track timeline: exact,
	// skew-free, and the right key for frame-accurate seeking. It cannot be
	// compared across producers.
	T0 int64 `json:"t0"`
	T1 int64 `json:"t1"`

	// W0 and W1 bound the group in wallclock time, in Unix nanoseconds. This
	// is the cross-track correlation key — the only way to align a video track
	// with a sensor track from a different publisher.
	W0 int64 `json:"w0"`
	W1 int64 `json:"w1"`

	// Object is the storage key of the payload. Readers must take it from
	// here rather than deriving it, because sequences are gappy.
	Object string `json:"object"`

	// Size is the payload length in bytes.
	Size int64 `json:"size"`

	// MIME and Encoding override the track defaults for this group alone, and
	// are empty when the group matches the track. They exist so a track can
	// change encoding mid-stream without invalidating its history.
	MIME     string `json:"mime,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

// Duration returns the group's extent in media time units.
func (m GroupMeta) Duration() int64 { return m.T1 - m.T0 }

// Contains reports whether a media timestamp falls within the group.
func (m GroupMeta) Contains(mediaTime int64) bool {
	return mediaTime >= m.T0 && mediaTime < m.T1
}

// ContainsWallclock reports whether a Unix-nanosecond instant falls within the
// group.
func (m GroupMeta) ContainsWallclock(unixNano int64) bool {
	return unixNano >= m.W0 && unixNano < m.W1
}

func (m GroupMeta) validate() error {
	switch {
	case m.Epoch == 0:
		return fmt.Errorf("%w: epoch must be non-zero", ErrInvalidGroup)
	case m.T1 < m.T0:
		return fmt.Errorf("%w: media range [%d,%d) is inverted", ErrInvalidGroup, m.T0, m.T1)
	case m.W1 < m.W0:
		return fmt.Errorf("%w: wallclock range [%d,%d) is inverted", ErrInvalidGroup, m.W0, m.W1)
	case m.Size < 0:
		return fmt.Errorf("%w: negative size %d", ErrInvalidGroup, m.Size)
	}

	return nil
}
