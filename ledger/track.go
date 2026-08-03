package ledger

import (
	"context"
	"errors"
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

// TrackSchema describes a track's content at creation time. It is written once
// into the root manifest and never changes; anything that can vary between
// groups belongs in [GroupInfo] instead.
//
// It is a value rather than a set of arguments so a schema can be carried
// around — notably [TrackInfo] embeds one, so a second track can be created
// with the same schema as an existing one:
//
//	src, _ := ledger.Open(ctx, objects, "live/cam1/video", ledger.Config{})
//	meta := srcReader.Root()
//	ledger.Create(ctx, objects, "live/cam2/video", meta.TrackSchema, ledger.Config{})
//
// Do not confuse it with [Config], which carries deployment settings — a
// logger, a clock, the seal threshold — that belong to a process rather than to
// a track.
type TrackSchema struct {
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

func (c TrackSchema) validate() error {
	if c.Timescale == 0 {
		return fmt.Errorf("%w: timescale must be non-zero", ErrInvalidGroup)
	}
	if !c.TimeSource.valid() {
		return fmt.Errorf("%w: unknown time source %q", ErrInvalidGroup, c.TimeSource)
	}

	return nil
}

// TrackInfo describes a track: the schema fixed at creation and the producer
// epoch currently being written. It is what [Reader.Root] and [Writer.Root]
// return, in place of the on-disk manifest. How the history is laid out into
// sealed and open regions is deliberately not part of it — that is storage
// structure, not a property of the track.
//
// It is the track-level counterpart of [GroupInfo]: each of the two domain
// objects has an identity ([TrackPath], [GroupRef]) and a record describing it
// ([TrackInfo], [GroupInfo]).
type TrackInfo struct {
	// TrackSchema is the schema the track was created with, embedded so its
	// fields read directly (info.Timescale) and so the whole schema can be
	// handed back to [Create] to make another track like this one.
	TrackSchema

	// Track is the path identifying the track.
	Track TrackPath

	// Epoch is the producer lifetime currently being written. It advances when
	// a producer restarts its numbering; see [GroupRef.Epoch].
	Epoch uint64
}

// Config carries the deployment-level settings for a track. The zero value is
// usable: every field means a documented default when left zero, so the common
// case is [Create] or [Open] with an empty [Config].
//
// These belong to a deployment rather than to one track — a logger or a clock
// is the same for every track a process holds — which is why they live on the
// handle rather than on each [Track.Writer] or [Track.Reader] call.
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
// [Reader] is built from. A track is a persistent, named thing whose schema is
// fixed at creation; you do not open one to use it, you hold a reference and
// call its methods.
//
// Obtain one with [Create] for a new track (which fixes its schema, like
// [os.Create]) or [Open] for an existing one (like [os.Open]):
//
//	track, _ := ledger.Create(ctx, objects, "live/cam1/video", ledger.TrackSchema{
//		Timescale: 90000, TimeSource: ledger.TimeSourceFrame, MIME: "video/mp4",
//	}, ledger.Config{})
//	writer, _ := track.Writer(ctx)
//	reader, _ := track.Reader(ctx)
//
// A Track holds no resources and needs no closing. It is safe for concurrent
// use, but must not be modified once a Writer or Reader has been built from it.
type Track struct {
	store store.Store
	path  TrackPath

	sealThreshold int64
	clock         func() time.Time
	logger        *slog.Logger

	// root is the manifest loaded by Create or Open. A Writer starts from it
	// (and advances it as it seals); a Reader loads a fresh copy of its own so
	// it sees the current state.
	root        rootManifest
	rootVersion store.Version
}

// resolveConfig builds a Track carrying store, path, and the deployment settings
// from cfg with zero values resolved to their defaults. The root is loaded
// separately by [Create] and [Open].
func resolveConfig(s store.Store, path TrackPath, cfg Config) *Track {
	t := &Track{store: s, path: path}

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

// Create establishes a new track, like [os.Create]: it writes the root manifest
// that fixes the track's schema — Timescale, TimeSource, MIME, Encoding — and
// returns a reference to it. The schema is then immutable.
//
// A track is an immutable, append-only log rather than a writable file, so
// where os.Create truncates an existing file, Create refuses one: it returns
// [ErrTrackExists] if the track already has a root manifest.
func Create(ctx context.Context, s store.Store, path TrackPath, schema TrackSchema, cfg Config) (*Track, error) {
	if err := path.validate(); err != nil {
		return nil, err
	}
	if err := schema.validate(); err != nil {
		return nil, err
	}

	t := resolveConfig(s, path, cfg)

	root := rootManifest{
		Version:    manifestVersion,
		Track:      path,
		Timescale:  schema.Timescale,
		TimeSource: schema.TimeSource,
		MIME:       schema.MIME,
		Encoding:   schema.Encoding,
		Epoch:      1,
		OpenFrom:   0,
		CreatedAt:  t.clock().UnixNano(),
	}

	data, err := encodeManifest(root)
	if err != nil {
		return nil, err
	}

	version, err := s.Create(ctx, rootKey(path), data)
	if err != nil {
		if errors.Is(err, store.ErrExist) {
			return nil, fmt.Errorf("%w: %s", ErrTrackExists, path)
		}
		return nil, fmt.Errorf("ledger: create track %s: %w", path, err)
	}

	t.root, t.rootVersion = root, version
	return t, nil
}

// Open references an existing track, like [os.Open]: it reads the root manifest
// and returns a reference, failing with [ErrTrackNotFound] if the track does not
// exist.
//
// Unlike [os.Open], the returned [Track] is both read- and write-capable: a
// writer that crashed mid-append resumes through Open(...).Writer(), and any
// Track reads through [Track.Reader].
func Open(ctx context.Context, s store.Store, path TrackPath, cfg Config) (*Track, error) {
	if err := path.validate(); err != nil {
		return nil, err
	}

	t := resolveConfig(s, path, cfg)

	root, version, err := fetchRoot(ctx, s, path)
	if err != nil {
		return nil, err
	}

	t.root, t.rootVersion = root, version
	return t, nil
}
