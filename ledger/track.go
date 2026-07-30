package ledger

import (
	"fmt"
	"path"
	"strings"
)

// TrackPath identifies one track, using forward slashes to express hierarchy
// exactly as a broadcast path does: "live/camera1/video".
//
// The hierarchy is the only grouping mechanism. Tracks that belong to the same
// broadcast share a prefix, so the tracks of "live/camera1" are found by prefix
// rather than by a separate namespace object. That keeps one naming scheme
// instead of two and lets an object store's own prefix semantics do the work.
type TrackPath string

// String returns the path as a string.
func (p TrackPath) String() string { return string(p) }

// Prefix reports whether the track lives under dir, which is how the tracks of
// a single broadcast are enumerated.
func (p TrackPath) Prefix(dir string) bool {
	return strings.HasPrefix(string(p), strings.TrimSuffix(dir, "/")+"/")
}

// Validate reports whether the path is usable as an object key prefix. Paths
// must be relative, free of empty or dot segments, and free of backslashes —
// a backslash is a legal character in an S3 key but becomes a separator once
// the filesystem backend maps a key onto a path.
func (p TrackPath) Validate() error {
	s := string(p)
	switch {
	case s == "":
		return fmt.Errorf("%w: empty", ErrInvalidTrackPath)
	case strings.HasPrefix(s, "/"):
		return fmt.Errorf("%w: %q is absolute", ErrInvalidTrackPath, s)
	case strings.HasSuffix(s, "/"):
		return fmt.Errorf("%w: %q has a trailing slash", ErrInvalidTrackPath, s)
	case strings.ContainsRune(s, '\\'):
		return fmt.Errorf("%w: %q contains a backslash", ErrInvalidTrackPath, s)
	case path.Clean(s) != s:
		return fmt.Errorf("%w: %q is not clean", ErrInvalidTrackPath, s)
	// A leading parent segment survives path.Clean, so it needs its own check
	// or it would reach the backend as a key escaping the track's prefix.
	case s == ".." || strings.HasPrefix(s, "../"):
		return fmt.Errorf("%w: %q escapes its prefix", ErrInvalidTrackPath, s)
	}

	return nil
}

// TimeSource records where a group's media timestamps came from, because the
// answer varies by producer and a reader needs to know how much to trust them.
type TimeSource string

const (
	// TimeSourceFrame means media timestamps were carried by the data itself,
	// as moq-lite draft-05 and later do with per-frame timestamp deltas. These
	// are exact and immune to clock skew.
	TimeSourceFrame TimeSource = "frame"

	// TimeSourceIngest means the ledger's own clock supplied the timestamps,
	// because the producer carried none — moq-lite draft-04 and earlier, or a
	// raw sensor feed. These record arrival, not occurrence, so they are wrong
	// for any backfilled or re-ingested recording.
	TimeSourceIngest TimeSource = "ingest"
)

// Valid reports whether the time source is one this package understands.
func (s TimeSource) Valid() bool {
	return s == TimeSourceFrame || s == TimeSourceIngest
}

// TrackConfig describes a track at creation time. It is written once into the
// root manifest and never changes; anything that can vary between groups
// belongs in [GroupMeta] instead.
type TrackConfig struct {
	// Timescale is the number of media time units per second, following the
	// same convention as a media container or a moq-lite track — 90000 for
	// video, the sample rate for audio, often 1000 for everything else.
	// Without it every media timestamp in the manifest is dimensionless.
	Timescale uint32

	// TimeSource records the provenance of media timestamps.
	TimeSource TimeSource

	// MIME is the media type of the group payloads, if known.
	MIME string

	// Encoding names the container or serialization of the payloads —
	// "fmp4", "protobuf", "moq-frames" — so a reader knows how to parse a
	// group without inferring it.
	Encoding string
}

func (c TrackConfig) validate() error {
	if c.Timescale == 0 {
		return fmt.Errorf("%w: timescale must be non-zero", ErrInvalidGroup)
	}
	if !c.TimeSource.Valid() {
		return fmt.Errorf("%w: unknown time source %q", ErrInvalidGroup, c.TimeSource)
	}

	return nil
}
