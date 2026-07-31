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
// It anchors a group on two timelines rather than describing a closed interval.
// Groups are serial within an epoch, so the start of one is the end of the last
// — which makes [GroupMeta.T0] the only time value that must always be present.
// [GroupMeta.Duration] and [GroupMeta.W0] are optional, because not every
// producer knows them.
type GroupMeta struct {
	GroupRef

	// T0 is the group's start in media time, expressed in the track's
	// timescale units. It is the anchor: it orders the group within its epoch
	// and is what a media seek resolves against.
	//
	// Required.
	T0 int64 `json:"t0"`

	// Duration is the group's extent in media time, or zero when the producer
	// could not determine it.
	//
	// Optional. It is stored rather than derived from the next group's T0
	// because that derivation fails exactly where it matters: across a dropped
	// group it would silently span the gap, and the newest group has no
	// successor at all — which would keep the newest segment out of a live HLS
	// playlist until the following one landed. Derived views consume it
	// directly, as EXTINF in HLS and @d in a DASH SegmentTimeline.
	//
	// The true end of a group also depends on the last frame's own duration,
	// which a container may not expose; EXTINF carries the same fuzziness.
	Duration int64 `json:"duration,omitempty"`

	// W0 is the group's start in wallclock time, in Unix nanoseconds, or zero
	// when no wallclock anchor is available.
	//
	// Optional. Media time is exact but relative to one track's origin, so
	// wallclock is what makes a group comparable against a different producer
	// — it is the key that answers "the video and the sensor readings at
	// 14:32". A group without it still replays within its own track; it just
	// cannot be correlated across tracks.
	W0 int64 `json:"w0,omitempty"`

	// ObjectCount is the number of objects (frames) the group contains. It
	// lets a reader confirm it consumed the whole group, and lets an object
	// index be range-checked without fetching the payload.
	ObjectCount uint64 `json:"objectCount,omitempty"`

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

// HasDuration reports whether the producer supplied a media extent.
func (m GroupMeta) HasDuration() bool { return m.Duration > 0 }

// HasWallclock reports whether the group carries a wallclock anchor, and so
// whether it can be correlated against another track.
func (m GroupMeta) HasWallclock() bool { return m.W0 != 0 }

// MediaEnd returns the group's end in media time. When the duration is unknown
// it equals T0, so check [GroupMeta.HasDuration] before treating it as an end.
func (m GroupMeta) MediaEnd() int64 { return m.T0 + m.Duration }

// wallclockEnd returns the group's end in Unix nanoseconds for a track of the
// given timescale, reporting false when either anchor or extent is missing.
func (m GroupMeta) wallclockEnd(timescale uint32) (int64, bool) {
	if !m.HasWallclock() || !m.HasDuration() || timescale == 0 {
		return 0, false
	}

	return m.W0 + mediaToNanos(m.Duration, timescale), true
}

// mediaToNanos converts a media-time extent into nanoseconds.
func mediaToNanos(units int64, timescale uint32) int64 {
	return units * int64(nanosPerSecond) / int64(timescale)
}

const nanosPerSecond = 1_000_000_000

func (m GroupMeta) validate() error {
	switch {
	case m.Epoch == 0:
		return fmt.Errorf("%w: epoch must be non-zero", ErrInvalidGroup)
	case m.Duration < 0:
		return fmt.Errorf("%w: negative duration %d", ErrInvalidGroup, m.Duration)
	case m.W0 < 0:
		return fmt.Errorf("%w: negative wallclock %d", ErrInvalidGroup, m.W0)
	case m.Size < 0:
		return fmt.Errorf("%w: negative size %d", ErrInvalidGroup, m.Size)
	}

	return nil
}
