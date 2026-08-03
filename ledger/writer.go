package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
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

// Writer appends groups to one producer lifetime of a track.
//
// A Writer is bound to a single epoch, which it stamps onto every group. The
// epoch is not something a caller picks: a writer opens at the track's latest
// epoch, and a producer that restarts calls [Writer.NewEpoch] to begin the next
// one. Within an epoch the writer is single-writer by design — two concurrent
// writers do not corrupt it (immutable objects and conditional creates make the
// loser's writes fail) but they produce errors rather than interleaving
// cleanly.
//
// Writer is safe for concurrent use.
type Writer struct {
	track   *Track
	objects store.Store
	path    TrackPath
	epoch   uint64
	schema  TrackSchema

	sealThreshold int64
	now           func() time.Time
	logger        *slog.Logger

	mu          sync.Mutex
	logRoot     epochLogRoot
	logVersion  store.Version
	headVersion store.Version

	// nextDelta is the delta number the next commit will claim.
	nextDelta uint64
	// openGroups accumulates the rows committed since the last seal. Rows are
	// small, so holding them avoids re-fetching the open region at seal time.
	openGroups []GroupInfo
	// openBytes tracks the encoded size of the open region against the seal
	// threshold.
	openBytes int64

	// last is the most recently committed group, kept so the next append can
	// be checked against it. hasLast distinguishes "no group yet" from a
	// zero-valued group.
	last    GroupInfo
	hasLast bool
}

// recover replays the epoch's open region so the seal threshold and group rows
// are accurate, and establishes the next delta to claim.
//
// It is the writer half of crash recovery. A writer that crashed mid-append
// resumes without losing committed groups and without a repair pass: head is a
// hint to skip ahead, and probing forward from the open region finds the first
// absent delta, which is the true tip — any delta that exists is committed,
// because a delta write is atomic and immutable.
func (w *Writer) recover(ctx context.Context) error {
	// head is only a hint. Trust it to skip ahead, never to stop early.
	from := w.logRoot.OpenFrom
	if h, headVersion, err := fetchHead(ctx, w.objects, w.path, w.epoch); err == nil {
		w.headVersion = headVersion
		if h.Delta >= from {
			from = h.Delta
		}
	} else if !errors.Is(err, store.ErrNotExist) {
		return err
	}

	for n := w.logRoot.OpenFrom; ; n++ {
		data, _, err := w.objects.Get(ctx, deltaKey(w.path, w.epoch, n))
		if errors.Is(err, store.ErrNotExist) {
			if n < from {
				// A gap below the head pointer means deltas were lost, which
				// immutability should make impossible. Refuse rather than
				// silently truncating the epoch.
				return fmt.Errorf("ledger: track %s epoch %d: delta %d missing below head %d", w.path, w.epoch, n, from)
			}
			w.nextDelta = n
			break
		}
		if err != nil {
			return fmt.Errorf("ledger: track %s epoch %d: read delta %d: %w", w.path, w.epoch, n, err)
		}

		delta, err := decodeManifest(data, func(d deltaManifest) int { return d.Version })
		if err != nil {
			return err
		}

		w.openGroups = append(w.openGroups, delta.Groups...)
		w.openBytes += int64(len(data))
		if n := len(delta.Groups); n > 0 {
			w.last, w.hasLast = delta.Groups[n-1], true
		}
	}

	// A writer reopened after a seal has no open groups to recover the last
	// committed one from, so fall back to the newest sealed run's summary.
	// Only the media end matters for ordering the next append.
	if !w.hasLast && len(w.logRoot.Sealed) > 0 {
		newest := w.logRoot.Sealed[len(w.logRoot.Sealed)-1]
		w.last = GroupInfo{ID: newest.Last, MediaTime: newest.MediaEnd}
		w.hasLast = true
	}

	return nil
}

// Track returns the path being written.
func (w *Writer) Track() TrackPath { return w.path }

// Epoch returns the producer epoch this writer is currently appending to. It
// advances when [Writer.NewEpoch] begins a new lifetime.
func (w *Writer) Epoch() uint64 { return w.epoch }

// logRootCopy returns a defensive copy of the current epoch log root. Internal
// callers and tests need the full root — its sealed index and open region —
// which the projection [Writer.Root] hides.
func (w *Writer) logRootCopy() epochLogRoot {
	w.mu.Lock()
	defer w.mu.Unlock()

	root := w.logRoot
	root.Sealed = append([]sealedRef(nil), w.logRoot.Sealed...)

	return root
}

// Root returns the track's read-side metadata. For a writer it is a projection
// of the schema and the epoch being written; how the history is laid out on
// disk is not part of the public API. See [TrackInfo].
func (w *Writer) Root() TrackInfo {
	return TrackInfo{
		TrackSchema: w.schema,
		Track:       w.path,
		// A writer only ever holds the latest epoch, so its epoch is the
		// track's latest as the writer sees it.
		LatestEpoch: w.epoch,
	}
}

// Append stores payload as the next group in sequence, deriving the values a
// sequential producer does not track by hand: sequence increments by one, media
// time advances by the previous group's duration, and wallclock is stamped from
// the writer's clock.
//
// duration is this group's media extent — the one value, besides the payload,
// that the ledger cannot supply, because the core parses no payload format.
//
// Append is the common case: groups committed back to back. Use [Writer.AppendGroup]
// when a group is dropped (a gap is real data), when the producer's own sequence
// numbers must be preserved for live-replay alignment, or when the media anchor
// is not simply the previous group's end.
func (w *Writer) Append(ctx context.Context, duration int64, payload []byte) (GroupInfo, error) {
	w.mu.Lock()
	var seq uint64
	var mediaStart int64
	if w.hasLast {
		seq = w.last.ID.Sequence() + 1
		mediaStart = w.last.mediaEnd()
	}
	w.mu.Unlock()

	return w.AppendGroup(ctx, GroupInfo{
		ID:        NewGroupID(0, seq),
		MediaTime: mediaStart,
		Duration:  duration,
		Wallclock: w.now().UnixNano(),
	}, payload)
}

// AppendGroup stores a sealed group and commits it.
//
// The caller supplies the group's content because the core parses no wire
// format: media timestamps live inside the payload, and only an adapter that
// understands the encoding can extract them. The epoch is taken from the
// writer's lifetime and any epoch in meta.ID is ignored; only its sequence part
// is read. ObjectKey and Size are filled in here.
//
// Ordering is payload first, manifest second. A crash between the two leaves an
// orphaned payload that no manifest references — invisible to readers and
// reclaimable by garbage collection. The reverse order would leave a manifest
// pointing at an object that does not exist, which readers cannot recover from.
//
// AppendGroup returns the committed row.
func (w *Writer) AppendGroup(ctx context.Context, meta GroupInfo, payload []byte) (GroupInfo, error) {
	meta.ID = NewGroupID(w.epoch, meta.ID.Sequence())
	if err := meta.validate(); err != nil {
		return GroupInfo{}, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// A track that declares its timestamps come from the ledger's own clock is
	// stamped here. One that declares them frame-derived is left alone, so an
	// absent anchor stays absent rather than being invented.
	if meta.Wallclock == 0 && w.schema.TimeSource == TimeSourceIngest {
		meta.Wallclock = w.now().UnixNano()
	}

	// Re-appending the group just committed is a duplicate rather than a
	// timeline contradiction, and saying so is more useful than the ordering
	// error the check below would produce. It also saves a doomed round trip.
	if w.hasLast && meta.ID == w.last.ID {
		return GroupInfo{}, fmt.Errorf("%w: %s in %s", ErrGroupExists, meta.ID, w.path)
	}

	if err := w.checkOrder(meta); err != nil {
		return GroupInfo{}, err
	}

	meta.ObjectKey = groupKey(w.path, meta.ID)
	meta.Size = int64(len(payload))

	if _, err := w.objects.Create(ctx, meta.ObjectKey, payload); err != nil {
		if errors.Is(err, store.ErrExist) {
			return GroupInfo{}, fmt.Errorf("%w: %s in %s", ErrGroupExists, meta.ID, w.path)
		}
		return GroupInfo{}, fmt.Errorf("ledger: write group %s: %w", meta.ID, err)
	}

	delta := deltaManifest{
		Version:     manifestVersion,
		Seq:         w.nextDelta,
		Groups:      []GroupInfo{meta},
		CommittedAt: w.now().UnixNano(),
	}

	data, err := encodeManifest(delta)
	if err != nil {
		return GroupInfo{}, err
	}

	// This create is the commit point.
	if _, err := w.objects.Create(ctx, deltaKey(w.path, w.epoch, w.nextDelta), data); err != nil {
		if errors.Is(err, store.ErrExist) {
			// Another writer claimed this delta number. Immutability turned a
			// silent split-brain into a clean failure.
			return GroupInfo{}, fmt.Errorf("ledger: delta %d already committed on %s epoch %d: %w", w.nextDelta, w.path, w.epoch, err)
		}
		return GroupInfo{}, fmt.Errorf("ledger: commit delta %d: %w", w.nextDelta, err)
	}

	w.nextDelta++
	w.openGroups = append(w.openGroups, meta)
	w.openBytes += int64(len(data))
	w.last, w.hasLast = meta, true

	if err := w.publishHead(ctx, meta.ID); err != nil {
		// not actionable: head is a discovery cache. A reader that finds it
		// stale probes forward and catches up, and one that finds it missing
		// starts from the log root. Failing an append that is already durably
		// committed would be the worse outcome, so this is reported for
		// operators and otherwise dropped.
		w.logger.Debug("ledger: could not publish head", "track", w.path, "epoch", w.epoch, "error", err)
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

// NewEpoch begins a new producer lifetime, advancing this writer to the next
// epoch. The first group afterwards restarts the timeline and the sequence
// numbering, so a producer that resets on restart continues without colliding
// with the immutable groups it committed before.
//
// Producers reset their sequence numbering on restart, and because group objects
// are immutable a reused sequence would collide rather than overwrite. NewEpoch
// gives each lifetime its own keyspace.
func (w *Writer) NewEpoch(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	next := w.epoch + 1
	if err := w.track.createEpochLog(ctx, next); err != nil {
		return fmt.Errorf("ledger: begin epoch %d of %s: %w", next, w.path, err)
	}

	logRoot, logVersion, err := fetchEpochLog(ctx, w.objects, w.path, next)
	if err != nil {
		return err
	}

	w.epoch = next
	w.logRoot, w.logVersion = logRoot, logVersion
	w.headVersion = store.NoVersion
	w.nextDelta = 0
	w.openGroups = w.openGroups[:0]
	w.openBytes = 0
	w.last, w.hasLast = GroupInfo{}, false
	return nil
}

// checkOrder rejects a group that would contradict the one before it.
// Requires w.mu.
//
// Groups are serial within an epoch: each starts at or after the previous one
// ended. Enforcing that here is the point of storing an extent rather than an
// endpoint — a contradiction becomes a failed append instead of a seek that
// quietly returns the wrong group months later.
//
// The first group in an epoch has no predecessor, so there is nothing to
// contradict.
func (w *Writer) checkOrder(meta GroupInfo) error {
	if !w.hasLast {
		return nil
	}

	if end := w.last.mediaEnd(); meta.MediaTime < end {
		return fmt.Errorf("%w: group %s starts at %d, before group %s ends at %d",
			ErrGroupOutOfOrder, meta.ID, meta.MediaTime, w.last.ID, end)
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
// the log root at it, and reclaims the deltas it replaced.
//
// The order matters. The sealed manifest is written first, then the log root is
// swapped to reference it, and only then are the open deltas deleted. A crash
// at any point leaves the epoch readable: an unreferenced sealed manifest is
// ignored, and deltas that outlive their seal are simply redundant.
// Requires w.mu.
func (w *Writer) seal(ctx context.Context) error {
	if len(w.openGroups) == 0 {
		return nil
	}

	firstDelta, lastDelta := w.logRoot.OpenFrom, w.nextDelta-1
	seq := uint64(len(w.logRoot.Sealed)) + 1
	key := sealedKey(w.path, w.epoch, firstDelta, lastDelta)

	sealed := sealedManifest{
		Version:    manifestVersion,
		Track:      w.path,
		Epoch:      w.epoch,
		Seq:        seq,
		FirstDelta: firstDelta,
		LastDelta:  lastDelta,
		Groups:     append([]GroupInfo(nil), w.openGroups...),
		SealedAt:   w.now().UnixNano(),
	}

	data, err := encodeManifest(sealed)
	if err != nil {
		return err
	}

	// ErrExist is safe to ignore only because the key names the delta range:
	// an object already under this key covers exactly these deltas, so it holds
	// the same groups. A retry after a wider range has accumulated writes a
	// different key and leaves the earlier object unreferenced for collection.
	if _, err := w.objects.Create(ctx, key, data); err != nil && !errors.Is(err, store.ErrExist) {
		return fmt.Errorf("ledger: write sealed manifest %d-%d: %w", firstDelta, lastDelta, err)
	}

	if err := w.updateLogRoot(ctx, func(root *epochLogRoot) {
		root.Sealed = append(root.Sealed, sealed.summarize(key))
		root.OpenFrom = lastDelta + 1
	}); err != nil {
		return fmt.Errorf("ledger: publish sealed manifest %d-%d: %w", firstDelta, lastDelta, err)
	}

	w.openGroups = w.openGroups[:0]
	w.openBytes = 0

	// Reclaiming the superseded deltas is best-effort: they are now redundant
	// rather than harmful, and a failure here must not fail the seal.
	for n := firstDelta; n <= lastDelta; n++ {
		if err := w.objects.Delete(ctx, deltaKey(w.path, w.epoch, n)); err != nil {
			w.logger.Debug("ledger: could not reclaim sealed delta",
				"track", w.path, "epoch", w.epoch, "delta", n, "error", err)
		}
	}

	return nil
}

// updateLogRoot applies mutate to the epoch log root and swaps it in.
// Requires w.mu.
func (w *Writer) updateLogRoot(ctx context.Context, mutate func(*epochLogRoot)) error {
	next := w.logRoot
	next.Sealed = append([]sealedRef(nil), w.logRoot.Sealed...)
	mutate(&next)

	data, err := encodeManifest(next)
	if err != nil {
		return err
	}

	version, err := w.objects.Swap(ctx, epochLogKey(w.path, w.epoch), data, w.logVersion)
	if err != nil {
		return fmt.Errorf("ledger: update log root of %s epoch %d: %w", w.path, w.epoch, err)
	}

	w.logRoot, w.logVersion = next, version

	return nil
}

// publishHead advances the epoch's head pointer. Requires w.mu.
//
// head is a discovery cache: a reader that finds it stale probes forward and
// catches up, and a reader that finds it missing starts from the log root.
// Nothing about correctness depends on it.
//
// It returns any error rather than handling it, leaving the decision to the
// caller — which lets the swallow be explicit and keeps the failure visible to
// tests.
func (w *Writer) publishHead(ctx context.Context, latest GroupID) error {
	h := head{
		Version:   manifestVersion,
		Delta:     w.nextDelta - 1,
		Latest:    latest,
		UpdatedAt: w.now().UnixNano(),
	}

	data, err := encodeManifest(h)
	if err != nil {
		return err
	}

	version, err := w.objects.Swap(ctx, headKey(w.path, w.epoch), data, w.headVersion)
	if errors.Is(err, store.ErrVersionMismatch) || errors.Is(err, store.ErrNotExist) {
		// Someone else wrote head, or it vanished. Re-read and try once more;
		// beyond that, let the next append carry it forward.
		if _, current, getErr := fetchHead(ctx, w.objects, w.path, w.epoch); getErr == nil {
			version, err = w.objects.Swap(ctx, headKey(w.path, w.epoch), data, current)
		} else if errors.Is(getErr, store.ErrNotExist) {
			version, err = w.objects.Swap(ctx, headKey(w.path, w.epoch), data, store.NoVersion)
		}
	}
	if err != nil {
		return fmt.Errorf("ledger: publish head of %s epoch %d: %w", w.path, w.epoch, err)
	}

	w.headVersion = version

	return nil
}
