package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/okdaichi/qumo-ledger/objectstore"
)

// DefaultSealThreshold is how many bytes of open manifest accumulate before the
// open region is rotated into a sealed manifest.
//
// The number is chosen for read amplification rather than storage efficiency.
// Each open delta is a separate object, so a reader wanting recent history
// fetches one request per delta, while the same groups inside a sealed manifest
// cost a single request. 64 KiB is roughly three hundred group rows: about ten
// minutes of two-second video, or a few minutes of dense sensor data. Sealing
// is triggered by size rather than by time or group count because group
// duration is a property of the track — a hundred-hertz sensor feed and a
// ten-second-GOP video track have nothing in common except how fast they fill a
// manifest.
const DefaultSealThreshold = 64 << 10

// Writer appends groups to one track.
//
// A track has exactly one writer by design. Two concurrent writers do not
// corrupt a track — immutable objects and conditional creates make the loser's
// writes fail — but they will produce errors rather than interleaving cleanly.
//
// Writer is safe for concurrent use.
type Writer struct {
	store objectstore.Store
	track TrackPath

	sealThreshold int64
	now           func() time.Time
	logger        *slog.Logger

	mu          sync.Mutex
	root        RootManifest
	rootVersion objectstore.Version
	headVersion objectstore.Version

	// nextDelta is the delta number the next commit will claim.
	nextDelta uint64
	// openGroups accumulates the rows committed since the last seal. Rows are
	// small, so holding them avoids re-fetching the open region at seal time.
	openGroups []GroupMeta
	// openBytes tracks the encoded size of the open region against the seal
	// threshold.
	openBytes int64

	// last is the most recently committed group, kept so the next append can
	// be checked against it. hasLast distinguishes "no group yet" from a
	// zero-valued group.
	last    GroupMeta
	hasLast bool
}

// WriterOption configures a Writer.
type WriterOption func(*Writer)

// WithSealThreshold sets the open-manifest byte size that triggers a seal.
func WithSealThreshold(bytes int64) WriterOption {
	return func(w *Writer) {
		if bytes > 0 {
			w.sealThreshold = bytes
		}
	}
}

// WithClock replaces the wallclock source, for tests and for producers that
// supply their own notion of ingest time.
func WithClock(now func() time.Time) WriterOption {
	return func(w *Writer) {
		if now != nil {
			w.now = now
		}
	}
}

// WithLogger sets the logger used for events that do not affect correctness,
// such as a failed head update.
func WithLogger(logger *slog.Logger) WriterOption {
	return func(w *Writer) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// CreateTrack writes a new root manifest and returns a Writer positioned at the
// start of the track. It returns ErrTrackExists if the track already has one.
func CreateTrack(ctx context.Context, store objectstore.Store, track TrackPath, cfg TrackConfig, opts ...WriterOption) (*Writer, error) {
	if err := track.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	w := newWriter(store, track, opts)

	w.root = RootManifest{
		Version:    ManifestVersion,
		Track:      track,
		Timescale:  cfg.Timescale,
		TimeSource: cfg.TimeSource,
		MIME:       cfg.MIME,
		Encoding:   cfg.Encoding,
		Epoch:      1,
		OpenFrom:   0,
		CreatedAt:  w.now().UnixNano(),
	}

	data, err := encodeManifest(w.root)
	if err != nil {
		return nil, err
	}

	version, err := store.Create(ctx, rootKey(track), data)
	if err != nil {
		if errors.Is(err, objectstore.ErrExist) {
			return nil, fmt.Errorf("%w: %s", ErrTrackExists, track)
		}
		return nil, fmt.Errorf("ledger: create track %s: %w", track, err)
	}
	w.rootVersion = version

	return w, nil
}

// OpenWriter reopens an existing track and recovers its position.
//
// Recovery reads the head pointer for a starting guess and then probes forward
// until a delta is absent. The absent delta is the true tip: because a delta is
// immutable and written atomically, any delta that exists is committed, whether
// or not head knows about it. A writer that crashed mid-append therefore
// resumes without losing committed groups and without a repair pass.
func OpenWriter(ctx context.Context, store objectstore.Store, track TrackPath, opts ...WriterOption) (*Writer, error) {
	if err := track.Validate(); err != nil {
		return nil, err
	}

	w := newWriter(store, track, opts)

	root, version, err := fetchRoot(ctx, store, track)
	if err != nil {
		return nil, err
	}
	w.root, w.rootVersion = root, version

	// head is only a hint. Trust it to skip ahead, never to stop early.
	from := root.OpenFrom
	if head, headVersion, err := fetchHead(ctx, store, track); err == nil {
		w.headVersion = headVersion
		if head.Delta >= from {
			from = head.Delta
		}
	} else if !errors.Is(err, objectstore.ErrNotExist) {
		return nil, err
	}

	// Replay the open region so the seal threshold and group rows are accurate.
	for n := root.OpenFrom; ; n++ {
		data, _, err := store.Get(ctx, deltaKey(track, n))
		if errors.Is(err, objectstore.ErrNotExist) {
			if n < from {
				// A gap below the head pointer means deltas were lost, which
				// immutability should make impossible. Refuse rather than
				// silently truncating the track.
				return nil, fmt.Errorf("ledger: track %s: delta %d missing below head %d", track, n, from)
			}
			w.nextDelta = n
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ledger: track %s: read delta %d: %w", track, n, err)
		}

		delta, err := decodeManifest(data, func(d DeltaManifest) int { return d.Version })
		if err != nil {
			return nil, err
		}

		w.openGroups = append(w.openGroups, delta.Groups...)
		w.openBytes += int64(len(data))
		if n := len(delta.Groups); n > 0 {
			w.last, w.hasLast = delta.Groups[n-1], true
		}
	}

	// A writer reopened after a seal has no open groups to recover the last
	// committed one from, so fall back to the newest sealed run's summary.
	// Only the epoch and end matter for ordering the next append.
	if !w.hasLast && len(root.Sealed) > 0 {
		newest := root.Sealed[len(root.Sealed)-1]
		w.last = GroupMeta{GroupRef: newest.Last, T0: newest.T1}
		w.hasLast = true
	}

	return w, nil
}

func newWriter(store objectstore.Store, track TrackPath, opts []WriterOption) *Writer {
	w := &Writer{
		store:         store,
		track:         track,
		sealThreshold: DefaultSealThreshold,
		now:           time.Now,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Track returns the path being written.
func (w *Writer) Track() TrackPath { return w.track }

// Root returns a copy of the current root manifest.
func (w *Writer) Root() RootManifest {
	w.mu.Lock()
	defer w.mu.Unlock()

	root := w.root
	root.Sealed = append([]SealedRef(nil), w.root.Sealed...)

	return root
}

// AppendGroup stores a sealed group and commits it.
//
// The caller supplies a fully populated GroupMeta because the core parses no
// wire format: media timestamps live inside the payload, and only an adapter
// that understands the encoding can extract them. Object and Size are filled in
// here and any caller-supplied values are ignored.
//
// Ordering is payload first, manifest second. A crash between the two leaves an
// orphaned payload that no manifest references — invisible to readers and
// reclaimable by garbage collection. The reverse order would leave a manifest
// pointing at an object that does not exist, which readers cannot recover from.
//
// AppendGroup returns the committed row.
func (w *Writer) AppendGroup(ctx context.Context, meta GroupMeta, payload []byte) (GroupMeta, error) {
	if err := meta.validate(); err != nil {
		return GroupMeta{}, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// A track that declares its timestamps come from the ledger's own clock is
	// stamped here. One that declares them frame-derived is left alone, so an
	// absent anchor stays absent rather than being invented.
	if meta.W0 == 0 && w.root.TimeSource == TimeSourceIngest {
		meta.W0 = w.now().UnixNano()
	}

	// Re-appending the group just committed is a duplicate rather than a
	// timeline contradiction, and saying so is more useful than the ordering
	// error the check below would produce. It also saves a doomed round trip.
	if w.hasLast && meta.GroupRef == w.last.GroupRef {
		return GroupMeta{}, fmt.Errorf("%w: %s in %s", ErrGroupExists, meta.GroupRef, w.track)
	}

	if err := w.checkOrder(meta); err != nil {
		return GroupMeta{}, err
	}

	meta.Object = groupKey(w.track, meta.GroupRef)
	meta.Size = int64(len(payload))

	if _, err := w.store.Create(ctx, meta.Object, payload); err != nil {
		if errors.Is(err, objectstore.ErrExist) {
			return GroupMeta{}, fmt.Errorf("%w: %s in %s", ErrGroupExists, meta.GroupRef, w.track)
		}
		return GroupMeta{}, fmt.Errorf("ledger: write group %s: %w", meta.GroupRef, err)
	}

	delta := DeltaManifest{
		Version:     ManifestVersion,
		Seq:         w.nextDelta,
		Groups:      []GroupMeta{meta},
		CommittedAt: w.now().UnixNano(),
	}

	data, err := encodeManifest(delta)
	if err != nil {
		return GroupMeta{}, err
	}

	// This create is the commit point.
	if _, err := w.store.Create(ctx, deltaKey(w.track, w.nextDelta), data); err != nil {
		if errors.Is(err, objectstore.ErrExist) {
			// Another writer claimed this delta number. Immutability turned a
			// silent split-brain into a clean failure.
			return GroupMeta{}, fmt.Errorf("ledger: delta %d already committed on %s: %w", w.nextDelta, w.track, err)
		}
		return GroupMeta{}, fmt.Errorf("ledger: commit delta %d: %w", w.nextDelta, err)
	}

	w.nextDelta++
	w.openGroups = append(w.openGroups, meta)
	w.openBytes += int64(len(data))
	w.last, w.hasLast = meta, true

	if meta.Epoch > w.root.Epoch {
		if err := w.updateRoot(ctx, func(root *RootManifest) { root.Epoch = meta.Epoch }); err != nil {
			return meta, err
		}
	}

	if err := w.publishHead(ctx, meta.GroupRef); err != nil {
		// not actionable: head is a discovery cache. A reader that finds it
		// stale probes forward and catches up, and one that finds it missing
		// starts from the root. Failing an append that is already durably
		// committed would be the worse outcome, so this is reported for
		// operators and otherwise dropped.
		w.logger.Debug("ledger: could not publish head", "track", w.track, "error", err)
	}

	if w.openBytes >= w.sealThreshold {
		if err := w.seal(ctx); err != nil {
			// The group is committed and durable; only the rotation failed.
			// Report it so the caller can retry, but do not imply data loss.
			return meta, fmt.Errorf("ledger: group committed but seal failed: %w", err)
		}
	}

	return meta, nil
}

// checkOrder rejects a group that would contradict the one before it.
// Requires w.mu.
//
// Groups are serial within an epoch: each starts at or after the previous one
// ended. Enforcing that here is the point of storing an extent rather than an
// endpoint — a contradiction becomes a failed append instead of a seek that
// quietly returns the wrong group months later.
//
// A new epoch restarts the timeline, so no ordering is implied across one.
func (w *Writer) checkOrder(meta GroupMeta) error {
	if meta.Epoch < w.root.Epoch {
		return fmt.Errorf("%w: group %s is in epoch %d, behind the track's epoch %d",
			ErrGroupOutOfOrder, meta.GroupRef, meta.Epoch, w.root.Epoch)
	}
	if !w.hasLast || w.last.Epoch != meta.Epoch {
		return nil
	}

	if end := w.last.MediaEnd(); meta.T0 < end {
		return fmt.Errorf("%w: group %s starts at %d, before group %s ends at %d",
			ErrGroupOutOfOrder, meta.GroupRef, meta.T0, w.last.GroupRef, end)
	}

	return nil
}

// Seal rotates the open region into a sealed manifest immediately, rather than
// waiting for the size threshold. Sealing an empty open region does nothing.
func (w *Writer) Seal(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.seal(ctx)
}

// seal folds the open deltas into one immutable sealed manifest, points
// the root at it, and reclaims the deltas it replaced.
//
// The order matters. The sealed manifest is written first, then the root is
// swapped to reference it, and only then are the open deltas deleted. A crash
// at any point leaves the track readable: an unreferenced sealed manifest is
// ignored, and deltas that outlive their seal are simply redundant.
// Requires w.mu.
func (w *Writer) seal(ctx context.Context) error {
	if len(w.openGroups) == 0 {
		return nil
	}

	seq := uint64(len(w.root.Sealed)) + 1
	key := sealedKey(w.track, seq)

	sealed := SealedManifest{
		Version:    ManifestVersion,
		Track:      w.track,
		Seq:        seq,
		FirstDelta: w.root.OpenFrom,
		LastDelta:  w.nextDelta - 1,
		Groups:     append([]GroupMeta(nil), w.openGroups...),
		SealedAt:   w.now().UnixNano(),
	}

	data, err := encodeManifest(sealed)
	if err != nil {
		return err
	}

	if _, err := w.store.Create(ctx, key, data); err != nil && !errors.Is(err, objectstore.ErrExist) {
		return fmt.Errorf("ledger: write sealed manifest %d: %w", seq, err)
	}

	firstDelta, lastDelta := w.root.OpenFrom, w.nextDelta-1
	if err := w.updateRoot(ctx, func(root *RootManifest) {
		root.Sealed = append(root.Sealed, sealed.summarize(key))
		root.OpenFrom = lastDelta + 1
	}); err != nil {
		return fmt.Errorf("ledger: publish sealed manifest %d: %w", seq, err)
	}

	w.openGroups = w.openGroups[:0]
	w.openBytes = 0

	// Reclaiming the superseded deltas is best-effort: they are now redundant
	// rather than harmful, and a failure here must not fail the seal.
	for n := firstDelta; n <= lastDelta; n++ {
		if err := w.store.Delete(ctx, deltaKey(w.track, n)); err != nil {
			w.logger.Debug("ledger: could not reclaim sealed delta",
				"track", w.track, "delta", n, "error", err)
		}
	}

	return nil
}

// updateRoot applies mutate to the root manifest and swaps it in.
// Requires w.mu.
func (w *Writer) updateRoot(ctx context.Context, mutate func(*RootManifest)) error {
	next := w.root
	next.Sealed = append([]SealedRef(nil), w.root.Sealed...)
	mutate(&next)

	data, err := encodeManifest(next)
	if err != nil {
		return err
	}

	version, err := w.store.Swap(ctx, rootKey(w.track), data, w.rootVersion)
	if err != nil {
		return fmt.Errorf("ledger: update root of %s: %w", w.track, err)
	}

	w.root, w.rootVersion = next, version

	return nil
}

// publishHead advances the head pointer. Requires w.mu.
//
// head is a discovery cache: a reader that finds it stale probes forward and
// catches up, and a reader that finds it missing starts from the root. Nothing
// about correctness depends on it.
//
// It returns any error rather than handling it, leaving the decision to the
// caller — which lets the swallow be explicit and keeps the failure visible to
// tests.
func (w *Writer) publishHead(ctx context.Context, latest GroupRef) error {
	head := Head{
		Version:   ManifestVersion,
		Delta:     w.nextDelta - 1,
		Latest:    latest,
		UpdatedAt: w.now().UnixNano(),
	}

	data, err := encodeManifest(head)
	if err != nil {
		return err
	}

	version, err := w.store.Swap(ctx, headKey(w.track), data, w.headVersion)
	if errors.Is(err, objectstore.ErrVersionMismatch) || errors.Is(err, objectstore.ErrNotExist) {
		// Someone else wrote head, or it vanished. Re-read and try once more;
		// beyond that, let the next append carry it forward.
		if _, current, getErr := fetchHead(ctx, w.store, w.track); getErr == nil {
			version, err = w.store.Swap(ctx, headKey(w.track), data, current)
		} else if errors.Is(getErr, objectstore.ErrNotExist) {
			version, err = w.store.Swap(ctx, headKey(w.track), data, objectstore.NoVersion)
		}
	}
	if err != nil {
		return fmt.Errorf("ledger: publish head of %s: %w", w.track, err)
	}

	w.headVersion = version

	return nil
}
