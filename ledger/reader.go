package ledger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Reader reads a track. It needs nothing but object-store access — no ledger
// process has to be running for a Reader to seek or replay.
//
// A Reader spans every epoch, in order: a track reads as one ascending run of
// [GroupID]s, and epoch boundaries are invisible to a consumer. Advancing from
// one epoch to the next happens inside [Reader.Next] as each lifetime is
// drained.
//
// A Reader is single-consumer: it holds a cursor for streaming and is not safe
// for concurrent use. Concurrent consumers each open their own Reader, which is
// cheap (one root fetch).
//
// # Two read modes
//
// Bounded queries answer a snapshot and never touch the cursor:
// [Reader.RangeMedia] and [Reader.RangeWallclock] iterate the groups overlapping
// a window, [Reader.SeekMedia] and [Reader.SeekWallclock] resolve one instant to
// the group anchored at or before it, and [Reader.ReadGroup] fetches a payload.
// The media-time queries are per-epoch — media time resets at each epoch — so a
// window or seek runs against every epoch's own timeline; wallclock is absolute
// and comparable across all of them.
//
// Positioned streaming drives the cursor: a Seek method positions it, then
// [Reader.Next] returns each following group and [io.EOF] at the current tip.
// SeekMedia and SeekWallclock do double duty — they resolve the target group
// and position the cursor there, so the group they return is the first a
// following Next yields. [Reader.Position] returns the [GroupID] to resume from.
//
// The Seek methods differ in where they land, which their names carry. The
// time-based ones are inclusive — a caller asking about an instant wants the
// group covering it — while [Reader.SeekAfter] is exclusive, because a caller
// replaying from a recorded position has already handled that group:
//
//	SeekStart()          the first group          (free: no request)
//	SeekTip(ctx)         after everything committed
//	SeekAfter(ctx, id)   strictly after id
//	SeekMedia(ctx, t)    on the group covering t
//	SeekWallclock(ctx,t) on the group covering t
//
// Tailing is a poll loop the caller owns, because object stores do not push:
//
//	reader.SeekTip(ctx)
//	ticker := time.NewTicker(ledger.DefaultPollInterval)
//	defer ticker.Stop()
//	for {
//		group, err := reader.Next(ctx)
//		if errors.Is(err, io.EOF) {
//			select {
//			case <-ctx.Done():
//				return ctx.Err()
//			case <-ticker.C:
//				continue
//			}
//		}
//		if err != nil {
//			return err
//		}
//		// handle group
//	}
//
// # Iteration and errors
//
// The range methods yield an error alongside each value because every step is a
// request to the object store, so a failure part-way through is expected rather
// than exceptional. That error is terminal and yielded at most once: iteration
// stops immediately after it. A loop may return or break on a non-nil error
// without draining the rest.
//
//	for group, err := range reader.RangeWallclock(ctx, from, to) {
//		if err != nil {
//			return err
//		}
//		// ...
//	}
type Reader struct {
	objects store.Store
	path    TrackPath
	schema  TrackSchema

	// latest is the track's latest epoch as it stood when the Reader last read
	// the track root — a snapshot refreshed by [Reader.Refresh]. It is how Next
	// learns a new lifetime has appeared.
	latest uint64

	// epoch1Log is epoch 1's log root, cached at construction so [Reader.SeekStart]
	// can rewind for free.
	epoch1Log epochLogRoot

	// epoch and logRoot are the lifetime currently being streamed and its log
	// root. Next advances epoch when the current one is drained.
	epoch   uint64
	logRoot epochLogRoot

	// Cursor state within the current epoch (SeekStart/SeekTip/SeekAfter + Next).
	// Not safe for concurrent use.
	next   uint64
	idx    int
	batch  []GroupInfo
	misses int
	last   GroupID
}

// Reader opens the track for reading across every epoch. It loads the track
// root and epoch 1's log and nothing else: reading needs no recovery, so a
// reader joins cheaply.
func (t *Track) Reader(ctx context.Context) (*Reader, error) {
	t.mu.Lock()
	latest := t.root.LatestEpoch
	schema := t.root.schema()
	t.mu.Unlock()

	if latest == 0 {
		return nil, fmt.Errorf("%w: %s has no epochs", ErrEpochNotFound, t.path)
	}

	// A fresh Reader is positioned at the start of epoch 1, so Next drains the
	// whole track before tailing. Use SeekTip to skip history.
	r := &Reader{objects: t.store, path: t.path, schema: schema, latest: latest}
	if err := r.loadEpoch(ctx, 1); err != nil {
		return nil, err
	}
	r.epoch1Log = r.logRoot
	return r, nil
}

// loadEpoch fetches epoch's log root and resets the cursor to its start.
func (r *Reader) loadEpoch(ctx context.Context, epoch uint64) error {
	logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, epoch)
	if err != nil {
		return err
	}
	r.epoch = epoch
	r.logRoot = logRoot
	r.next, r.idx, r.batch = 0, 0, nil
	r.misses = 0
	return nil
}

// Track returns the path being read.
func (r *Reader) Track() TrackPath { return r.path }

// logRootCopy returns a defensive copy of the current epoch's log root. Tests
// need it to assert on the sealed index after a seal; the projection
// [Reader.Root] hides all of that.
func (r *Reader) logRootCopy() epochLogRoot {
	root := r.logRoot
	root.Sealed = append([]sealedRef(nil), r.logRoot.Sealed...)
	return root
}

// Root returns the track's read-side metadata. It is a projection of the
// schema and the latest epoch as of when this Reader last refreshed, not the
// log root itself: how the history is laid out on disk is not part of the
// public API. See [TrackInfo].
func (r *Reader) Root() TrackInfo {
	return TrackInfo{
		TrackSchema: r.schema,
		Track:       r.path,
		LatestEpoch: r.latest,
	}
}

// Refresh re-reads the track root and the current epoch's log root, picking up
// history sealed since the Reader was opened and any new epoch a producer has
// begun. Bounded seeks into rotated history need it; the open region is
// discovered by probing, so tailing does not — Next refreshes on its own
// schedule.
func (r *Reader) Refresh(ctx context.Context) error {
	root, _, err := fetchTrackRoot(ctx, r.objects, r.path)
	if err != nil {
		return err
	}
	r.latest = root.LatestEpoch

	logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, r.epoch)
	if err != nil {
		return err
	}
	r.logRoot = logRoot
	if r.epoch == 1 {
		r.epoch1Log = logRoot
	}
	return nil
}

// delta returns the nth delta manifest of epoch, or ErrNotCommitted if it does
// not exist yet. Absence is the normal signal that a reader has caught up with
// the writer, not a failure.
func (r *Reader) delta(ctx context.Context, epoch, n uint64) (deltaManifest, error) {
	data, _, err := r.objects.Get(ctx, deltaKey(r.path, epoch, n))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return deltaManifest{}, fmt.Errorf("%w: %s epoch %d delta %d", ErrNotCommitted, r.path, epoch, n)
		}
		return deltaManifest{}, fmt.Errorf("ledger: read delta %d: %w", n, err)
	}

	return decodeManifest(data, func(d deltaManifest) int { return d.Version })
}

// sealed returns a sealed manifest of epoch by its reference in the log root.
func (r *Reader) sealed(ctx context.Context, epoch uint64, ref sealedRef) (sealedManifest, error) {
	data, _, err := r.objects.Get(ctx, ref.Key)
	if err != nil {
		return sealedManifest{}, fmt.Errorf("ledger: read sealed manifest %q: %w", ref.Key, err)
	}

	sealed, err := decodeManifest(data, func(m sealedManifest) int { return m.Version })
	if err != nil {
		return sealedManifest{}, err
	}

	// The manifest names its own track, epoch, and range, so disagreement with
	// the reference means the object is not the one the log root meant to point
	// at.
	switch {
	case sealed.Track != r.path:
		return sealedManifest{}, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, ref.Key, sealed.Track, r.path)
	case sealed.Epoch != epoch:
		return sealedManifest{}, fmt.Errorf("%w: %q holds epoch %d, expected %d",
			ErrManifestMismatch, ref.Key, sealed.Epoch, epoch)
	case sealed.FirstDelta != ref.FirstDelta || sealed.LastDelta != ref.LastDelta:
		return sealedManifest{}, fmt.Errorf("%w: %q covers deltas %d-%d, expected %d-%d",
			ErrManifestMismatch, ref.Key, sealed.FirstDelta, sealed.LastDelta, ref.FirstDelta, ref.LastDelta)
	}

	return sealed, nil
}

// ReadGroup fetches a group's payload.
//
// It reads [GroupInfo.ObjectKey] rather than deriving a key, because producer
// sequences are gappy and a derived key can name a group that was dropped.
func (r *Reader) ReadGroup(ctx context.Context, meta GroupInfo) ([]byte, error) {
	data, _, err := r.objects.Get(ctx, meta.ObjectKey)
	if err != nil {
		return nil, fmt.Errorf("ledger: read group %s: %w", meta.ID, err)
	}

	return data, nil
}

// RangeMedia iterates the groups overlapping the half-open media-time window
// [from, to), in the track's timescale units, across every epoch.
//
// Media time resets at each epoch, so a window is run against each lifetime's
// own timeline; the same offset can match in more than one epoch. A group with
// a known duration is included when it overlaps the window, so the group
// *containing* from is returned even though it starts earlier — which is what a
// player needs to decode into the window. A group with no duration is a point,
// and is included only when its anchor falls inside.
func (r *Reader) RangeMedia(ctx context.Context, from, to int64) iter.Seq2[GroupInfo, error] {
	if from >= to {
		return emptyGroups
	}

	return filterGroups(
		r.walk(ctx, func(ref sealedRef) bool { return ref.MediaStart < to && ref.MediaEnd > from }),
		func(group GroupInfo) bool { return group.overlapsMedia(from, to) },
	)
}

// RangeWallclock iterates the groups overlapping the half-open wallclock window
// [from, to), in Unix nanoseconds, across every epoch. Groups carrying no
// wallclock anchor are skipped: they cannot be placed on a shared timeline.
//
// This is the cross-track query. Running it over a video track and a sensor
// track with the same window is what lines the two recordings up.
func (r *Reader) RangeWallclock(ctx context.Context, from, to int64) iter.Seq2[GroupInfo, error] {
	if from >= to {
		return emptyGroups
	}

	timescale := r.schema.Timescale

	return filterGroups(
		r.walk(ctx, func(ref sealedRef) bool {
			// sealedRef.WallclockEnd is the last anchor rather than an end, so a group
			// sitting on it may still reach into the window. Only a run that
			// begins after the window can be ruled out. A run with no anchors
			// at all cannot be ruled out either.
			return ref.WallclockStart == 0 || ref.WallclockStart < to
		}),
		func(group GroupInfo) bool { return group.overlapsWallclock(from, to, timescale) },
	)
}

// DefaultPollInterval is a suggested interval for a tailing poll loop over
// [Reader.Next].
//
// Object stores do not push, so following a track means polling, and the
// interval is the visibility latency floor for a tailing reader — one reason
// the ledger is not a live path. Next is non-blocking and returns [io.EOF] at
// the tip, leaving the wait to the caller; this constant is the value the cmd
// and most callers use.
const DefaultPollInterval = 500_000_000 // 500ms, as a count of nanoseconds

// rootRecheckEvery is how many consecutive empty polls pass between re-reads of
// the roots while a Reader is stalled at the tip.
//
// A stalled Reader needs the roots only to notice a seal or a new epoch that
// happened since it last read them, and neither can happen before the delta it
// is waiting for is even committed. Re-reading on every poll therefore learns
// nothing and wastes requests, so it is rationed.
const rootRecheckEvery = 8

// SeekStart positions the Reader at the first group, so a following
// [Reader.Next] loop drains the whole track before tailing. It costs no
// request: epoch 1's log root is cached at construction.
func (r *Reader) SeekStart() {
	r.epoch = 1
	r.logRoot = r.epoch1Log
	r.next, r.idx, r.batch = 0, 0, nil
	r.last, r.misses = 0, 0
}

// SeekTip positions the Reader after everything currently committed in the
// latest epoch, so only groups committed after the call arrive. The position is
// derived from the head pointer, which may lag the true tip, so a tail from it
// can replay a few groups that were already committed — delivery is at least
// once by design.
func (r *Reader) SeekTip(ctx context.Context) error {
	if err := r.loadEpoch(ctx, r.latest); err != nil {
		return err
	}
	switch h, _, err := fetchHead(ctx, r.objects, r.path, r.latest); {
	case errors.Is(err, store.ErrNotExist):
		// Nothing committed yet: the start of the epoch is already the tip.
		r.next = 0
	case err != nil:
		return err
	default:
		r.next = h.Delta + 1
	}

	r.idx, r.batch = 0, nil
	r.last, r.misses = 0, 0
	return nil
}

// SeekAfter positions the Reader strictly after id, so a following
// [Reader.Next] loop resumes without re-yielding the group id names. Pair it
// with [Reader.Position] to resume across a restart.
//
// The exclusivity is what makes it a resume: [Reader.SeekMedia] and
// [Reader.SeekWallclock] land *on* the group they resolve, because a caller
// asking about an instant wants the group covering it. A caller replaying from a
// recorded position has already handled that group and wants the next one — and
// cannot name it, since producer sequences are gappy.
//
// The zero id precedes every group, so SeekAfter with one is [Reader.SeekStart].
func (r *Reader) SeekAfter(ctx context.Context, id GroupID) error {
	if id == 0 {
		// Nothing precedes the start, and rewinding costs no request where
		// locating the first group would fetch a sealed manifest.
		r.SeekStart()
		return nil
	}

	epoch := id.Epoch()
	if epoch > r.latest {
		epoch = r.latest
	}
	if err := r.loadEpoch(ctx, epoch); err != nil {
		return err
	}

	seg, idx, after, err := r.locateAfter(ctx, r.logRoot, id.Sequence())
	if err != nil {
		return err
	}
	// Whether or not a following group exists, `after` is the right place to
	// resume: a real segment's tail, or the tip when id is past everything in
	// this epoch — Next then advances into the next one.
	r.batch, r.idx, r.next = seg, idx, after
	r.last, r.misses = 0, 0
	return nil
}

// position sets the cursor to a resolved segment in the current epoch. Shared
// by the time-based seeks.
func (r *Reader) position(epoch uint64, seg []GroupInfo, idx int, after uint64) {
	r.epoch = epoch
	r.batch, r.idx, r.next = seg, idx, after
	r.last, r.misses = 0, 0
}

// Next advances to and returns the next group in commit order, across epochs.
// It returns [io.EOF] when the Reader has reached the current tip — caught up,
// for now — and a real error only when the store failed.
//
// io.EOF is not terminal: a later group may have been committed, so the next
// call re-probes. It mutates nothing but the empty-poll counter, so a caller
// may poll indefinitely.
//
// When the current epoch is drained and a newer one exists, Next steps into it
// and continues, so a single reader spans a producer restart. A delta that has
// been sealed and reclaimed since the Reader last read the log root is still
// served: its groups moved into a sealed run, and Next notices on its rationed
// re-read rather than hanging on the deleted delta.
func (r *Reader) Next(ctx context.Context) (GroupInfo, error) {
	for {
		if r.idx < len(r.batch) {
			group := r.batch[r.idx]
			r.idx++
			r.last = group.ID
			return group, nil
		}

		// batch exhausted: load the next segment in the current epoch.

		if r.next < r.logRoot.OpenFrom {
			// The next delta is below the open region: either it has always been
			// sealed, or it was sealed away while this Reader was parked. Either
			// way its groups live in a sealed run.
			ref, ok := sealedCovering(r.logRoot, r.next)
			if !ok {
				// A gap below OpenFrom that no sealed run covers: skip to the
				// open region and keep going.
				r.next = r.logRoot.OpenFrom
				continue
			}
			sealed, err := r.sealed(ctx, r.epoch, ref)
			if err != nil {
				return GroupInfo{}, err
			}
			r.batch, r.idx, r.next, r.misses = sealed.Groups, 0, ref.LastDelta+1, 0
			continue
		}

		delta, err := r.delta(ctx, r.epoch, r.next)
		if err == nil {
			r.batch, r.idx, r.next, r.misses = delta.Groups, 0, r.next+1, 0
			continue
		}
		if errors.Is(err, ErrNotCommitted) {
			// The current epoch is drained. Step into the next one if it exists.
			if r.epoch < r.latest {
				if err := r.loadEpoch(ctx, r.epoch+1); err != nil {
					return GroupInfo{}, err
				}
				continue
			}
			// At the latest epoch's tip: the delta is absent. It may simply not
			// be committed yet, may have been sealed away, or a new epoch may
			// have begun. A root re-read settles which, but only matters
			// occasionally, so it is rationed.
			r.misses++
			if r.misses == 1 || r.misses%rootRecheckEvery == 0 {
				if e := r.Refresh(ctx); e != nil {
					return GroupInfo{}, e
				}
			}
			return GroupInfo{}, io.EOF
		}
		return GroupInfo{}, err
	}
}

// Position returns the [GroupID] of the most recently yielded group, for saving
// across a restart. Pass it to [Reader.SeekAfter] to resume strictly after it.
//
// Save it as text — [GroupID.String] on the way out, [ParseGroupID] on the way
// back — so a consumer's stored position is one opaque string:
//
//	save(reader.Position().String())     // "e000001-g00000042"
//	id, _ := ledger.ParseGroupID(saved)
//	reader.SeekAfter(ctx, id)
//
// Before any group has been yielded it is the zero [GroupID], which SeekAfter
// reads as "from the start."
func (r *Reader) Position() GroupID { return r.last }

// sealedCovering returns the sealed run holding a delta number, if any.
func sealedCovering(root epochLogRoot, delta uint64) (sealedRef, bool) {
	for _, ref := range root.Sealed {
		if delta >= ref.FirstDelta && delta <= ref.LastDelta {
			return ref, true
		}
	}

	return sealedRef{}, false
}

// walk iterates the track in commit order across every epoch, fetching only the
// sealed runs that include accepts. A nil include fetches every run.
func (r *Reader) walk(ctx context.Context, include func(sealedRef) bool) iter.Seq2[GroupInfo, error] {
	return func(yield func(GroupInfo, error) bool) {
		for epoch := uint64(1); epoch <= r.latest; epoch++ {
			logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, epoch)
			if err != nil {
				yield(GroupInfo{}, err)
				return
			}

			for _, ref := range logRoot.Sealed {
				if include != nil && !include(ref) {
					continue
				}

				sealed, err := r.sealed(ctx, epoch, ref)
				if err != nil {
					yield(GroupInfo{}, err)
					return
				}
				for _, group := range sealed.Groups {
					if !yield(group, nil) {
						return
					}
				}
			}

			for n := logRoot.OpenFrom; ; n++ {
				delta, err := r.delta(ctx, epoch, n)
				if errors.Is(err, ErrNotCommitted) {
					break
				}
				if err != nil {
					yield(GroupInfo{}, err)
					return
				}
				for _, group := range delta.Groups {
					if !yield(group, nil) {
						return
					}
				}
			}
		}
	}
}

// filterGroups drops groups that keep rejects, passing errors straight through.
func filterGroups(seq iter.Seq2[GroupInfo, error], keep func(GroupInfo) bool) iter.Seq2[GroupInfo, error] {
	return func(yield func(GroupInfo, error) bool) {
		for group, err := range seq {
			if err != nil {
				yield(GroupInfo{}, err)
				return
			}
			if !keep(group) {
				continue
			}
			if !yield(group, nil) {
				return
			}
		}
	}
}

// emptyGroups yields nothing, for a window that cannot contain anything.
func emptyGroups(func(GroupInfo, error) bool) {}

// SeekWallclock returns the group anchored at or before a Unix-nanosecond
// instant, skipping groups that carry no wallclock anchor.
//
// Wallclock is the cross-track key: seeking two tracks to the same instant is
// what makes correlated replay — video alongside the sensor readings recorded
// with it — possible at all. For seeking within one track's own clock, use
// [Reader.SeekMedia], which is exact and immune to skew.
//
// The search runs newest-epoch-first, so when more than one epoch has a group at
// the instant the latest lifetime wins. SeekWallclock also positions the cursor
// at the group it returns, so a following [Reader.Next] loop plays forward from
// it.
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupInfo, error) {
	return r.seekTime(ctx, unixNano,
		func(g GroupInfo) (int64, bool) { return g.Wallclock, g.hasWallclock() },
		func(g GroupInfo, ts uint32) (int64, bool) { return g.wallclockEnd(ts) },
		func(ref sealedRef) (int64, bool) { return ref.WallclockStart, ref.WallclockStart != 0 },
	)
}

// SeekMedia returns the group anchored at or before a media timestamp, in the
// track's timescale units.
//
// Media time resets at each epoch, so the search runs newest-epoch-first and
// the latest lifetime that has a group at the timestamp wins. SeekMedia also
// positions the cursor at the group it returns — which is what a player wants:
// land on or before the target, then decode forward with [Reader.Next].
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupInfo, error) {
	return r.seekTime(ctx, mediaTime,
		func(g GroupInfo) (int64, bool) { return g.MediaTime, true },
		func(g GroupInfo, _ uint32) (int64, bool) { return g.mediaEnd(), g.hasDuration() },
		func(ref sealedRef) (int64, bool) { return ref.MediaStart, true },
	)
}

// seekTime resolves the last group anchored at or before target across epochs,
// newest-epoch-first, and positions the cursor there. It returns the location
// so the time-based seeks position without a second fetch.
//
// It resolves against anchors rather than testing containment, which is both
// what a player wants — land on or before the target and decode forward — and
// what keeps seeking correct when a producer supplies no duration. Duration is
// consulted only at the end, to reject a target that falls past a known end.
func (r *Reader) seekTime(
	ctx context.Context,
	target int64,
	anchor func(GroupInfo) (int64, bool),
	end func(GroupInfo, uint32) (int64, bool),
	start func(sealedRef) (int64, bool),
) (GroupInfo, error) {
	timescale := r.schema.Timescale

	for epoch := r.latest; epoch >= 1; epoch-- {
		logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, epoch)
		if err != nil {
			return GroupInfo{}, err
		}

		best, bestSeg, bestIdx, bestNext, found := r.seekWithinEpoch(ctx, epoch, logRoot, target, anchor, end, start)
		if !found {
			continue
		}
		if at, ok := end(best, timescale); ok && target >= at {
			return GroupInfo{}, fmt.Errorf("%w: %d falls past the end of %s in %s epoch %d", ErrGroupNotFound, target, best.ID, r.path, epoch)
		}
		r.position(epoch, bestSeg, bestIdx, bestNext)
		return best, nil
	}

	return GroupInfo{}, fmt.Errorf("%w: nothing in %s is anchored at or before %d", ErrGroupNotFound, r.path, target)
}

// seekWithinEpoch resolves the last group anchored at or before target within a
// single epoch's log. The open region is examined before the sealed history
// because it holds the newest data; sealed runs are then walked in reverse
// until one begins at or before the target.
func (r *Reader) seekWithinEpoch(
	ctx context.Context,
	epoch uint64,
	root epochLogRoot,
	target int64,
	anchor func(GroupInfo) (int64, bool),
	end func(GroupInfo, uint32) (int64, bool),
	start func(sealedRef) (int64, bool),
) (best GroupInfo, bestSeg []GroupInfo, bestIdx int, bestNext uint64, found bool) {
	var bestAt int64
	consider := func(seg []GroupInfo, i int, after uint64) {
		group := seg[i]
		at, ok := anchor(group)
		if !ok || at > target {
			return
		}
		if !found || at >= bestAt {
			best, bestSeg, bestIdx, bestNext = group, seg, i, after
			bestAt = at
			found = true
		}
	}

	for n := root.OpenFrom; ; n++ {
		delta, err := r.delta(ctx, epoch, n)
		if errors.Is(err, ErrNotCommitted) {
			break
		}
		if err != nil {
			return GroupInfo{}, nil, 0, 0, false
		}
		for i := range delta.Groups {
			consider(delta.Groups, i, n+1)
		}
	}

	for i := len(root.Sealed) - 1; !found && i >= 0; i-- {
		ref := root.Sealed[i]

		// A run beginning after the target cannot hold a match. Its summary is
		// already in the root, so skipping costs no fetch. A run carrying no
		// wallclock at all reports no start, and is searched rather than
		// assumed empty.
		if at, ok := start(ref); ok && at > target {
			continue
		}

		sealed, err := r.sealed(ctx, epoch, ref)
		if err != nil {
			return GroupInfo{}, nil, 0, 0, false
		}
		for j := range sealed.Groups {
			consider(sealed.Groups, j, ref.LastDelta+1)
		}
	}

	return best, bestSeg, bestIdx, bestNext, found
}

// locateAfter finds the first committed group strictly after seq within the
// current epoch — the resume point for a [Reader] positioned past a recorded
// sequence. It returns the segment the group lives in, its index within it, and
// the delta number to resume at once that segment is drained. When nothing
// follows seq it returns a nil segment with next set to the tip, so the caller
// parks at the end and Next steps into the next epoch.
func (r *Reader) locateAfter(ctx context.Context, root epochLogRoot, seq uint64) (seg []GroupInfo, idx int, next uint64, err error) {
	for _, sref := range root.Sealed {
		// A run whose last group does not exceed seq holds nothing after it.
		if seq >= sref.Last.Sequence() {
			continue
		}
		sealed, err := r.sealed(ctx, r.epoch, sref)
		if err != nil {
			return nil, 0, 0, err
		}
		for i, g := range sealed.Groups {
			if seq < g.ID.Sequence() {
				return sealed.Groups, i, sref.LastDelta + 1, nil
			}
		}
	}

	for n := root.OpenFrom; ; n++ {
		delta, err := r.delta(ctx, r.epoch, n)
		if errors.Is(err, ErrNotCommitted) {
			// Nothing follows seq; park at the tip.
			return nil, 0, n, nil
		}
		if err != nil {
			return nil, 0, 0, err
		}
		for i, g := range delta.Groups {
			if seq < g.ID.Sequence() {
				return delta.Groups, i, n + 1, nil
			}
		}
	}
}

func fetchTrackRoot(ctx context.Context, objects store.Store, track TrackPath) (trackRoot, store.Version, error) {
	data, version, err := objects.Get(ctx, rootKey(track))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return trackRoot{}, store.NoVersion, fmt.Errorf("%w: %s", ErrTrackNotFound, track)
		}
		return trackRoot{}, store.NoVersion, fmt.Errorf("ledger: read root of %s: %w", track, err)
	}

	root, err := decodeManifest(data, func(m trackRoot) int { return m.Version })
	if err != nil {
		return trackRoot{}, store.NoVersion, err
	}
	if root.Track != track {
		return trackRoot{}, store.NoVersion, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, rootKey(track), root.Track, track)
	}

	return root, version, nil
}

func fetchEpochLog(ctx context.Context, objects store.Store, track TrackPath, epoch uint64) (epochLogRoot, store.Version, error) {
	data, version, err := objects.Get(ctx, epochLogKey(track, epoch))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return epochLogRoot{}, store.NoVersion, fmt.Errorf("%w: %s epoch %d", ErrEpochNotFound, track, epoch)
		}
		return epochLogRoot{}, store.NoVersion, fmt.Errorf("ledger: read log of %s epoch %d: %w", track, epoch, err)
	}

	root, err := decodeManifest(data, func(m epochLogRoot) int { return m.Version })
	if err != nil {
		return epochLogRoot{}, store.NoVersion, err
	}
	if root.Track != track {
		return epochLogRoot{}, store.NoVersion, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, epochLogKey(track, epoch), root.Track, track)
	}
	if root.Epoch != epoch {
		return epochLogRoot{}, store.NoVersion, fmt.Errorf("%w: %q holds epoch %d, expected %d",
			ErrManifestMismatch, epochLogKey(track, epoch), root.Epoch, epoch)
	}

	return root, version, nil
}

func fetchHead(ctx context.Context, objects store.Store, track TrackPath, epoch uint64) (head, store.Version, error) {
	data, version, err := objects.Get(ctx, headKey(track, epoch))
	if err != nil {
		return head{}, store.NoVersion, err
	}

	h, err := decodeManifest(data, func(v head) int { return v.Version })
	if err != nil {
		return head{}, store.NoVersion, err
	}

	return h, version, nil
}
