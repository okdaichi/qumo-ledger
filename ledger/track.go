package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
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
//	info := src.Root()
//	ledger.Create(ctx, objects, "live/cam2/video", info.TrackSchema, ledger.Config{})
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

// TrackInfo describes a track: the schema fixed at creation and the latest
// producer epoch. It is what [Reader.Root] and [Writer.Root] return, in place
// of the on-disk manifest. How each epoch's history is laid out into sealed and
// open regions is deliberately not part of it — that is storage structure, not
// a property of the track.
//
// It is the track-level counterpart of [GroupInfo]: each of the two domain
// objects has an identity ([TrackPath], [GroupID]) and a record describing it
// ([TrackInfo], [GroupInfo]).
type TrackInfo struct {
	// TrackSchema is the schema the track was created with, embedded so its
	// fields read directly (info.Timescale) and so the whole schema can be
	// handed back to [Create] to make another track like this one.
	TrackSchema

	// Track is the path identifying the track.
	Track TrackPath

	// LatestEpoch is the newest producer epoch the track has. A producer that
	// restarts opens the next one through [Writer.NewEpoch]; see [GroupID].
	// A [Reader] or [Writer] reports the track's LatestEpoch as it was when
	// constructed, which a later [Track.Reload] can refresh.
	LatestEpoch uint64
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
// use: opening or advancing an epoch updates the cached track root under a
// lock, and each [Writer] and [Reader] is self-contained once built.
type Track struct {
	store store.Store
	path  TrackPath

	sealThreshold int64
	clock         func() time.Time
	logger        *slog.Logger

	// mu guards root and rootVersion. They change only when an epoch is
	// created; everything else a Reader or Writer touches lives in its epoch's
	// own log, not here.
	mu          sync.Mutex
	root        trackRoot
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
// creates its first epoch, returning a reference. The schema is then immutable.
//
// A track is an immutable, append-only log rather than a writable file, so
// where os.Create truncates an existing file, Create refuses one: it returns
// [ErrTrackExists] if the track already has a root manifest. The first epoch's
// log is written alongside the root, so a freshly created track is already
// writable and readable; should that write fail, the root is still durable,
// and the first [Track.Writer] creates the log lazily.
func Create(ctx context.Context, s store.Store, path TrackPath, schema TrackSchema, cfg Config) (*Track, error) {
	if err := path.validate(); err != nil {
		return nil, err
	}
	if err := schema.validate(); err != nil {
		return nil, err
	}

	t := resolveConfig(s, path, cfg)

	root := trackRoot{
		Version:     manifestVersion,
		Track:       path,
		Timescale:   schema.Timescale,
		TimeSource:  schema.TimeSource,
		MIME:        schema.MIME,
		Encoding:    schema.Encoding,
		LatestEpoch: 1,
		CreatedAt:   t.clock().UnixNano(),
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

	if err := t.createEpochLog(ctx, 1); err != nil {
		return nil, fmt.Errorf("ledger: create epoch 1 of %s: %w", path, err)
	}

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

	root, version, err := fetchTrackRoot(ctx, s, path)
	if err != nil {
		return nil, err
	}

	t.root, t.rootVersion = root, version
	return t, nil
}

// LatestEpoch returns the newest producer epoch the track has, from the cached
// track root. A follower that needs to notice a new epoch refreshes the cache
// with [Track.Reload].
func (t *Track) LatestEpoch() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.root.LatestEpoch
}

// Root returns the track's read-side metadata from the cached track root: the
// schema fixed at creation and the latest epoch. It is the same projection
// [Reader.Root] and [Writer.Root] make; a follower refreshes it through
// [Track.Reload].
func (t *Track) Root() TrackInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	return TrackInfo{
		TrackSchema: t.root.schema(),
		Track:       t.path,
		LatestEpoch: t.root.LatestEpoch,
	}
}

// Reload re-reads the track root, refreshing the latest epoch the cache
// reports. A tailing consumer calls it to detect that a producer has opened a
// new epoch.
func (t *Track) Reload(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	root, version, err := fetchTrackRoot(ctx, t.store, t.path)
	if err != nil {
		return err
	}
	t.root, t.rootVersion = root, version
	return nil
}

// Writer opens the track's latest epoch for appending and recovers its
// position. A producer that restarts begins the next epoch through
// [Writer.NewEpoch]; until then every append is written under the current one.
//
// Recovery reads the head pointer for a starting guess and then probes forward
// until a delta is absent. The absent delta is the true tip: because a delta is
// immutable and written atomically, any delta that exists is committed, whether
// or not head knows about it. A writer that crashed mid-append therefore
// resumes without losing committed groups and without a repair pass.
//
// Within an epoch the writer is single-writer by design: two concurrent writers
// do not corrupt an epoch, but the loser's writes fail rather than interleave.
func (t *Track) Writer(ctx context.Context) (*Writer, error) {
	t.mu.Lock()
	latest := t.root.LatestEpoch
	schema := t.root.schema()
	t.mu.Unlock()

	if latest == 0 {
		return nil, fmt.Errorf("%w: %s has no epochs", ErrEpochNotFound, t.path)
	}

	w := t.newWriter(latest, schema)

	logRoot, logVersion, err := fetchEpochLog(ctx, t.store, t.path, latest)
	if errors.Is(err, ErrEpochNotFound) {
		// The root claims this epoch exists but its log does not: a creation
		// that did not finish. Create it now and re-fetch.
		if err := t.createEpochLog(ctx, latest); err != nil {
			return nil, err
		}
		logRoot, logVersion, err = fetchEpochLog(ctx, t.store, t.path, latest)
	}
	if err != nil {
		return nil, err
	}
	w.logRoot, w.logVersion = logRoot, logVersion

	if err := w.recover(ctx); err != nil {
		return nil, err
	}
	return w, nil
}

// newWriter builds a writer bound to epoch, carrying the track's resolved
// settings. The log root and recovered position are filled in by [Writer.recover]
// after the log root has been fetched.
func (t *Track) newWriter(epoch uint64, schema TrackSchema) *Writer {
	return &Writer{
		track:         t,
		objects:       t.store,
		path:          t.path,
		epoch:         epoch,
		schema:        schema,
		sealThreshold: t.sealThreshold,
		now:           t.clock,
		logger:        t.logger,
	}
}

// createEpochLog writes an epoch's log root if it does not exist and records
// the epoch in the track root. It is idempotent: a log another writer created,
// or an orphan left by a crashed creation, is adopted rather than treated as an
// error. It takes the track lock itself, so it is safe to call from a
// [Writer.NewEpoch].
func (t *Track) createEpochLog(ctx context.Context, epoch uint64) error {
	root := epochLogRoot{
		Version:   manifestVersion,
		Track:     t.path,
		Epoch:     epoch,
		OpenFrom:  0,
		CreatedAt: t.clock().UnixNano(),
	}
	data, err := encodeManifest(root)
	if err != nil {
		return err
	}

	if _, err := t.store.Create(ctx, epochLogKey(t.path, epoch), data); err != nil {
		if !errors.Is(err, store.ErrExist) {
			return fmt.Errorf("ledger: create log of %s epoch %d: %w", t.path, epoch, err)
		}
		// ErrExist: another writer created it (or an orphan from a crash).
		// Adopt it and fall through to record the epoch in the track root.
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return t.advanceTrackRootLocked(ctx, epoch)
}

// advanceTrackRootLocked bumps the track root's LatestEpoch to epoch,
// idempotently. On a version mismatch another writer has moved the root; it is
// re-read and adopted if it has reached epoch, otherwise the bump is retried
// once. The caller must hold t.mu.
func (t *Track) advanceTrackRootLocked(ctx context.Context, epoch uint64) error {
	if t.root.LatestEpoch >= epoch {
		return nil
	}

	bump := func(base trackRoot, version store.Version) (trackRoot, store.Version, error) {
		next := base
		next.LatestEpoch = epoch
		data, err := encodeManifest(next)
		if err != nil {
			return trackRoot{}, store.NoVersion, err
		}
		v, err := t.store.Swap(ctx, rootKey(t.path), data, version)
		if err != nil {
			return trackRoot{}, store.NoVersion, err
		}
		return next, v, nil
	}

	next, version, err := bump(t.root, t.rootVersion)
	if errors.Is(err, store.ErrVersionMismatch) || errors.Is(err, store.ErrNotExist) {
		// Another writer moved the root, or (impossibly) it vanished. Re-fetch
		// and adopt if it has reached epoch; otherwise retry the bump once.
		current, curVersion, getErr := fetchTrackRoot(ctx, t.store, t.path)
		if getErr != nil {
			return getErr
		}
		t.root, t.rootVersion = current, curVersion
		if current.LatestEpoch >= epoch {
			return nil
		}
		next, version, err = bump(current, curVersion)
	}
	if err != nil {
		return fmt.Errorf("ledger: advance track root of %s to epoch %d: %w", t.path, epoch, err)
	}
	t.root, t.rootVersion = next, version
	return nil
}
