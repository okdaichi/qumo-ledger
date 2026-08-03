package ledger

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// GroupRef identifies a group within a track.
//
// Sequence alone is not an identity. Producers reset their numbering when they
// restart, and because group objects are immutable a reused sequence would
// collide rather than overwrite — leaving the track wedged. Epoch separates
// each producer lifetime into its own keyspace, so a restart continues cleanly
// while the producer's original sequence numbers survive intact for clients
// aligning replay against a live relay.
//
// Treat [GroupRef.String] as the identifier to hold onto: it is the stable,
// portable form, and [ParseGroupRef] recovers the ref from it. Persist that
// string in a state file, a database column, or a log line rather than the two
// numbers, and the pair stays an implementation detail you never have to
// reassemble. The fields are exported because a GroupRef is embedded in
// [GroupInfo], which is the manifest's own JSON shape; reading Epoch and
// Sequence directly is for interpreting a ref, not for building one — the
// writer assigns Epoch (see [Writer.AdvanceEpoch]).
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

// ParseGroupRef parses the "e<epoch>-g<sequence>" form produced by
// [GroupRef.String], so a consumer can persist a position with String and resume
// with ParseGroupRef. Both the zero-padded form ("e000001-g00000042") and an
// unpadded one ("e1-g42") are accepted; the empty string is the zero GroupRef.
//
// It is a free function rather than an encoding.TextUnmarshaler method on
// purpose: GroupInfo embeds GroupRef, and a TextMarshaler/TextUnmarshaler there
// would make encoding/json serialize every manifest row as a string and corrupt
// the wire format.
func ParseGroupRef(s string) (GroupRef, error) {
	if s == "" {
		return GroupRef{}, nil
	}

	epochText, ok := strings.CutPrefix(s, "e")
	if !ok {
		return GroupRef{}, fmt.Errorf("ledger: malformed group ref %q: missing epoch", s)
	}
	epochText, seqText, ok := strings.Cut(epochText, "-g")
	if !ok {
		return GroupRef{}, fmt.Errorf("ledger: malformed group ref %q: missing sequence", s)
	}

	epoch, err := strconv.ParseUint(epochText, 10, 64)
	if err != nil {
		return GroupRef{}, fmt.Errorf("ledger: malformed group ref %q: %w", s, err)
	}
	sequence, err := strconv.ParseUint(seqText, 10, 64)
	if err != nil {
		return GroupRef{}, fmt.Errorf("ledger: malformed group ref %q: %w", s, err)
	}

	return GroupRef{Epoch: epoch, Sequence: sequence}, nil
}

// GroupInfo is a group's manifest row: everything a reader needs to decide
// whether to fetch the payload, without fetching it.
//
// It anchors a group on two timelines rather than describing a closed interval.
// Groups are serial within an epoch, so the start of one is the end of the last
// — which makes [GroupInfo.MediaTime] the only time value that must always be present.
// [GroupInfo.Duration] and [GroupInfo.Wallclock] are optional, because not every
// producer knows them.
type GroupInfo struct {
	GroupRef

	// MediaTime is the group's start in media time, expressed in the track's
	// timescale units. It is the anchor: it orders the group within its epoch
	// and is what a media seek resolves against.
	//
	// Required.
	MediaTime int64 `json:"mediaTime"`

	// Duration is the group's extent in media time, or zero when the producer
	// could not determine it.
	//
	// Optional. It is stored rather than derived from the next group's anchor
	// because that derivation fails exactly where it matters: across a dropped
	// group it would silently span the gap, and the newest group has no
	// successor at all — which would keep the newest segment out of a live HLS
	// playlist until the following one landed. Derived views consume it
	// directly, as EXTINF in HLS and @d in a DASH SegmentTimeline.
	//
	// The true end of a group also depends on the last frame's own duration,
	// which a container may not expose; EXTINF carries the same fuzziness.
	Duration int64 `json:"duration,omitempty"`

	// Wallclock is the group's start in wallclock time, in Unix nanoseconds, or zero
	// when no wallclock anchor is available.
	//
	// Optional. Media time is exact but relative to one track's origin, so
	// wallclock is what makes a group comparable against a different producer
	// — it is the key that answers "the video and the sensor readings at
	// 14:32". A group without it still replays within its own track; it just
	// cannot be correlated across tracks.
	Wallclock int64 `json:"wallclock,omitempty"`

	// ObjectCount is the number of objects (frames) the group contains. It
	// lets a reader confirm it consumed the whole group, and lets an object
	// index be range-checked without fetching the payload.
	ObjectCount uint64 `json:"objectCount,omitempty"`

	// Object is the storage key of the payload. Readers must take it from
	// here rather than deriving it, because sequences are gappy.
	ObjectKey string `json:"objectKey"`

	// Size is the payload length in bytes.
	Size int64 `json:"size"`

	// MIME and Encoding override the track defaults for this group alone, and
	// are empty when the group matches the track. They exist so a track can
	// change encoding mid-stream without invalidating its history.
	MIME     string `json:"mime,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

// hasDuration reports whether the producer supplied a media extent.
func (m GroupInfo) hasDuration() bool { return m.Duration > 0 }

// hasWallclock reports whether the group carries a wallclock anchor, and so
// whether it can be correlated against another track.
func (m GroupInfo) hasWallclock() bool { return m.Wallclock != 0 }

// mediaEnd returns the group's end in media time. It equals MediaTime when no duration
// was supplied, so callers must establish that one was.
//
// A group whose range would overflow an int64 is refused at append, so this
// cannot wrap for anything the ledger stored.
func (m GroupInfo) mediaEnd() int64 { return m.MediaTime + m.Duration }

// wallclockEnd returns the group's end in Unix nanoseconds for a track of the
// given timescale, reporting false when an anchor or extent is missing, or when
// the result would not fit in an int64.
func (m GroupInfo) wallclockEnd(timescale uint32) (int64, bool) {
	if !m.hasWallclock() || !m.hasDuration() {
		return 0, false
	}

	nanos, ok := mediaToNanos(m.Duration, timescale)
	if !ok || m.Wallclock > math.MaxInt64-nanos {
		return 0, false
	}

	return m.Wallclock + nanos, true
}

// overlapsMedia reports whether the group intersects the half-open media-time
// window [from, to). A group with a duration counts if any of it falls inside,
// so the group containing from is kept even though it starts earlier. Without a
// duration a group is a point at its anchor.
func (m GroupInfo) overlapsMedia(from, to int64) bool {
	if !m.hasDuration() {
		return m.MediaTime >= from && m.MediaTime < to
	}

	return m.MediaTime < to && m.mediaEnd() > from
}

// overlapsWallclock is overlapsMedia on the shared timeline. A group with no
// anchor cannot be placed on it and never matches.
func (m GroupInfo) overlapsWallclock(from, to int64, timescale uint32) bool {
	if !m.hasWallclock() {
		return false
	}

	end, ok := m.wallclockEnd(timescale)
	if !ok {
		return m.Wallclock >= from && m.Wallclock < to
	}

	return m.Wallclock < to && end > from
}

// mediaToNanos converts a media-time extent into nanoseconds, reporting false
// when the conversion would overflow.
//
// The obvious units*1e9/timescale wraps sooner than it looks: a track with
// Timescale 1 needs only about 9.2e9 units, which is well inside the range of a
// long recording. Dividing first keeps the remainder term bounded — a remainder
// is always below the timescale, so at most ~4.3e18 for a uint32, comfortably
// inside an int64 — and leaves only the whole-seconds term to range-check.
func mediaToNanos(units int64, timescale uint32) (int64, bool) {
	if timescale == 0 || units < 0 {
		return 0, false
	}

	scale := int64(timescale)
	seconds, remainder := units/scale, units%scale

	if seconds > math.MaxInt64/nanosPerSecond {
		return 0, false
	}
	whole := seconds * nanosPerSecond

	fraction := remainder * nanosPerSecond / scale
	if whole > math.MaxInt64-fraction {
		return 0, false
	}

	return whole + fraction, true
}

const nanosPerSecond = 1_000_000_000

func (m GroupInfo) validate() error {
	switch {
	case m.Epoch == 0:
		return fmt.Errorf("%w: epoch must be non-zero", ErrInvalidGroup)
	case m.Duration < 0:
		return fmt.Errorf("%w: negative duration %d", ErrInvalidGroup, m.Duration)
	case m.Wallclock < 0:
		return fmt.Errorf("%w: negative wallclock %d", ErrInvalidGroup, m.Wallclock)
	case m.Size < 0:
		return fmt.Errorf("%w: negative size %d", ErrInvalidGroup, m.Size)
	// MediaEnd is exported and cannot report failure, so a range that would
	// wrap is refused here instead. A wrapped end reads as *before* its own
	// start, which would let the ordering check pass anything after it.
	case m.Duration > 0 && m.MediaTime > math.MaxInt64-m.Duration:
		return fmt.Errorf("%w: media range from %d for %d units overflows", ErrInvalidGroup, m.MediaTime, m.Duration)
	}

	return nil
}
