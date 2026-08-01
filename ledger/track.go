package ledger

import (
	"fmt"
	"log/slog"
	"path"
	"strings"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
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

// validate reports whether the path is usable as an object key prefix. Paths
// must be relative, free of empty or dot segments, and free of backslashes —
// a backslash is a legal character in an S3 key but becomes a separator once
// the filesystem backend maps a key onto a path.
//
// Every entry point validates for the caller, so this stays unexported.
func (p TrackPath) validate() error {
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

// valid reports whether the time source is one this package understands.
func (s TimeSource) valid() bool {
	return s == TimeSourceFrame || s == TimeSourceIngest
}

// TrackConfig describes a track at creation time. It is written once into the
// root manifest and never changes; anything that can vary between groups
// belongs in [GroupInfo] instead.
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
	if !c.TimeSource.valid() {
		return fmt.Errorf("%w: unknown time source %q", ErrInvalidGroup, c.TimeSource)
	}

	return nil
}

// TrackMeta is a track's read-side metadata: the schema fixed at creation and
// the producer epoch currently being written. It is the public projection of the
// root, so [Reader.Root] and [Writer.Root] return one of these rather than the
// on-disk manifest. How the history is laid out into sealed and open regions is
// deliberately not part of it — that is storage structure, not track metadata.
type TrackMeta struct {
	// Track is the path identifying the track.
	Track TrackPath

	// Timescale, TimeSource, MIME and Encoding mirror the [TrackConfig] the
	// track was created with.
	Timescale  uint32
	TimeSource TimeSource
	MIME       string
	Encoding   string

	// Epoch is the producer lifetime currently being written. It advances when
	// a producer restarts its numbering; see [GroupRef.Epoch].
	Epoch uint64
}

// Config carries the deployment-level settings for a [Track]. The zero value is
// usable: every field means a documented default when left zero, so the common
// case is [NewTrack] with an empty [Config].
//
// These belong to a deployment rather than to one track — a logger or a clock
// is the same for every track a process holds — which is why they live on the
// handle rather than on each [Track.Create], [Track.Writer] or [Track.Reader]
// call.
type Config struct {
	// SealThreshold is how many bytes of open manifest accumulate before the
	// open region is rotated into a sealed manifest. Zero means
	// [DefaultSealThreshold].
	SealThreshold int64

	// Clock supplies wallclock time, for tests and for producers with their
	// own notion of ingest time. Nil means [time.Now].
	Clock func() time.Time

	// Logger receives events that do not affect correctness, such as a failed
	// head update. Nil means [slog.Default].
	Logger *slog.Logger
}

// Track is a reference to one track in a store — the handle a [Writer] or
// [Reader] is built from. A track, like a table in a database, is a persistent
// named thing whose schema is fixed at creation: you do not open one to use it,
// you hold a reference and call its methods.
//
// Build one with [NewTrack]:
//
//	track := ledger.NewTrack(objects, "live/cam1/video", ledger.Config{})
//	writer, _ := track.Writer(ctx)
//	reader, _ := track.Reader(ctx)
//
// [Track.Create] is the one-time act of establishing a new track — the
// equivalent of CREATE TABLE, which sets the track's schema — and returns a
// [Writer] at its start. To append to or read an existing track, hold a Track
// from [NewTrack] and call [Track.Writer] or [Track.Reader]; there is no
// separate open step.
//
// A Track holds no resources and needs no closing. It is safe for concurrent
// use, but must not be modified once a Writer or Reader has been built from it.
type Track struct {
	store store.Store
	path  TrackPath

	sealThreshold int64
	clock         func() time.Time
	logger        *slog.Logger
}

// NewTrack returns a reference to path within store, configured by cfg.
//
// It does no I/O and never fails: a Track is a reference, not an open. A track
// that does not yet exist is discovered when [Track.Create], [Track.Writer], or
// [Track.Reader] reaches the store, so constructing a Track for a track that may
// or may not exist costs nothing.
//
// The zero-value fields of cfg are resolved to their defaults here, so a Track
// always carries usable settings.
func NewTrack(store store.Store, path TrackPath, cfg Config) *Track {
	t := &Track{
		store: store,
		path:  path,
	}

	t.sealThreshold = cfg.SealThreshold
	if t.sealThreshold <= 0 {
		t.sealThreshold = DefaultSealThreshold
	}

	t.clock = cfg.Clock
	if t.clock == nil {
		t.clock = time.Now
	}

	t.logger = cfg.Logger
	if t.logger == nil {
		t.logger = slog.Default()
	}

	return t
}

// check reports whether the track is usable. Returning an error rather than
// letting a nil store panic keeps the common mistake legible.
func (t *Track) check() error {
	if t.store == nil {
		return ErrNoStore
	}

	return nil
}
