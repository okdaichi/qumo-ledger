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
//
// Positioned streaming drives the cursor: a Seek method positions it, then
// [Reader.Next] returns each following group and [io.EOF] at the current tip.
// SeekMedia and SeekWallclock do double duty — they resolve the target group
// and position the cursor there, so the group they return is the first a
// following Next yields. [Reader.Position] returns the [GroupRef] to resume
// from; [ParseGroupRef] round-trips its text form across a restart.
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
	track   TrackPath

	root rootManifest

	// Cursor state for streaming (SeekStart/SeekTip/SeekGroup + Next). Not
	// safe for concurrent use.
	next   uint64
	idx    int
	batch  []GroupInfo
	misses int
	last   GroupRef
}

// Reader opens the track for reading. It loads the root manifest and nothing
// else: reading needs no recovery, so a reader joins cheaply.
func (t *Track) Reader(ctx context.Context) (*Reader, error) {
	root, _, err := fetchRoot(ctx, t.store, t.path)
	if err != nil {
		return nil, err
	}

	// A fresh Reader is positioned at the start, so Next drains the whole track
	// before tailing. Use SeekTip to skip history.
	return &Reader{objects: t.store, track: t.path, root: root}, nil
}

// Track returns the path being read.
func (r *Reader) Track() TrackPath { return r.track }

// rootManifest returns a defensive copy of the cached root. Internal callers
// need the full root — its sealed index and open region — to seek and walk; the
// projection [Reader.Root] returns hides all of that.
func (r *Reader) rootManifest() rootManifest {
	root := r.root
	root.Sealed = append([]sealedRef(nil), r.root.Sealed...)

	return root
}

// Root returns the track's read-side metadata. It is a projection of the cached
// root, not the root itself: how the history is laid out on disk is not part of
// the public API. See [TrackMeta].
func (r *Reader) Root() TrackMeta {
	return r.rootManifest().meta()
}

// Refresh re-reads the root manifest, picking up history sealed since the
// Reader was opened. Bounded seeks into rotated history need it; the open
// region is discovered by probing, so tailing does not — Next re-reads the root
// on its own schedule.
func (r *Reader) Refresh(ctx context.Context) error {
	root, _, err := fetchRoot(ctx, r.objects, r.track)
	if err != nil {
		return err
	}
	r.root = root
	return nil
}

// Delta returns the nth delta manifest, or ErrNotCommitted if it does not
// exist yet. Absence is the normal signal that a reader has caught up with the
// writer, not a failure.
func (r *Reader) delta(ctx context.Context, n uint64) (deltaManifest, error) {
	data, _, err := r.objects.Get(ctx, deltaKey(r.track, n))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return deltaManifest{}, fmt.Errorf("%w: %s delta %d", ErrNotCommitted, r.track, n)
		}
		return deltaManifest{}, fmt.Errorf("ledger: read delta %d: %w", n, err)
	}

	return decodeManifest(data, func(d deltaManifest) int { return d.Version })
}

// Sealed returns a sealed manifest by its reference in the root.
func (r *Reader) sealed(ctx context.Context, ref sealedRef) (sealedManifest, error) {
	data, _, err := r.objects.Get(ctx, ref.Key)
	if err != nil {
		return sealedManifest{}, fmt.Errorf("ledger: read sealed manifest %q: %w", ref.Key, err)
	}

	sealed, err := decodeManifest(data, func(m sealedManifest) int { return m.Version })
	if err != nil {
		return sealedManifest{}, err
	}

	// The manifest names its own track and range, so disagreement with the
	// reference means the object is not the one the root meant to point at.
	switch {
	case sealed.Track != r.track:
		return sealedManifest{}, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, ref.Key, sealed.Track, r.track)
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
		return nil, fmt.Errorf("ledger: read group %s: %w", meta.GroupRef, err)
	}

	return data, nil
}

// RangeMedia iterates the groups overlapping the half-open media-time window
// [from, to), in the track's timescale units.
//
// A group with a known duration is included when it overlaps the window, so the
// group *containing* from is returned even though it starts earlier — which is
// what a player needs to decode into the window. A group with no duration is a
// point, and is included only when its anchor falls inside.
func (r *Reader) RangeMedia(ctx context.Context, from, to int64) iter.Seq2[GroupInfo, error] {
	if from >= to {
		return emptyGroups
	}

	return filterGroups(
		// MediaEnd is a true end, so a run finishing at or before the window cannot
		// contribute and need not be fetched.
		r.walk(ctx, func(ref sealedRef) bool { return ref.MediaStart < to && ref.MediaEnd > from }),
		func(group GroupInfo) bool { return group.overlapsMedia(from, to) },
	)
}

// RangeWallclock iterates the groups overlapping the half-open wallclock window
// [from, to), in Unix nanoseconds. Groups carrying no wallclock anchor are
// skipped: they cannot be placed on a shared timeline.
//
// This is the cross-track query. Running it over a video track and a sensor
// track with the same window is what lines the two recordings up.
func (r *Reader) RangeWallclock(ctx context.Context, from, to int64) iter.Seq2[GroupInfo, error] {
	if from >= to {
		return emptyGroups
	}

	timescale := r.rootManifest().Timescale

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
// the root manifest while a Reader is stalled at the tip.
//
// A stalled Reader needs the root only to notice a seal that happened since it
// last read one, and a seal cannot happen before the delta it is waiting for is
// even committed. Re-reading on every poll therefore learns nothing and wastes
// a request, so it is rationed.
const rootRecheckEvery = 8

// SeekStart positions the Reader at the first group, so a following [Reader.Next]
// loop drains the whole recording before tailing.
func (r *Reader) SeekStart() {
	r.next, r.idx, r.batch = 0, 0, nil
	r.last, r.misses = GroupRef{}, 0
}

// SeekTip positions the Reader after everything currently committed, so only
// groups committed after the call arrive. The position is derived from the head
// pointer, which may lag the true tip, so a tail from it can replay a few
// groups that were already committed — delivery is at least once by design.
func (r *Reader) SeekTip(ctx context.Context) error {
	switch h, _, err := fetchHead(ctx, r.objects, r.track); {
	case errors.Is(err, store.ErrNotExist):
		// Nothing committed yet: the start of the track is already the tip.
		r.next = 0
	case err != nil:
		return err
	default:
		r.next = h.Delta + 1
	}

	r.idx, r.batch = 0, nil
	r.last, r.misses = GroupRef{}, 0
	return nil
}

// SeekGroup positions the Reader strictly after ref, so a following
// [Reader.Next] loop resumes without re-yielding the group ref names. Pair it
// with [Reader.Position] to resume across a restart.
func (r *Reader) SeekGroup(ctx context.Context, ref GroupRef) error {
	seg, idx, after, err := r.locateAfter(ctx, r.root, ref)
	if err != nil {
		return err
	}
	// Whether or not a following group exists, `after` is the right place to
	// resume: a real segment's tail, or the tip when ref is past everything.
	r.batch, r.idx, r.next = seg, idx, after
	r.last, r.misses = GroupRef{}, 0
	return nil
}

// position sets the cursor to a resolved segment. Shared by the time-based
// seeks.
func (r *Reader) position(seg []GroupInfo, idx int, after uint64) {
	r.batch, r.idx, r.next = seg, idx, after
	r.last, r.misses = GroupRef{}, 0
}

// Next advances to and returns the next group in commit order. It returns
// [io.EOF] when the Reader has reached the current tip — caught up, for now —
// and a real error only when the store failed.
//
// io.EOF is not terminal: a later group may have been committed, so the next
// call re-probes. It mutates nothing but the empty-poll counter, so a caller
// may poll indefinitely.
//
// A delta that has been sealed and reclaimed since the Reader last read the
// root is still served: its groups moved into a sealed run, and Next notices on
// its rationed root re-read rather than hanging on the deleted delta.
func (r *Reader) Next(ctx context.Context) (GroupInfo, error) {
	for {
		if r.idx < len(r.batch) {
			group := r.batch[r.idx]
			r.idx++
			r.last = group.GroupRef
			return group, nil
		}

		// batch exhausted: load the next segment.

		if r.next < r.root.OpenFrom {
			// The next delta is below the open region: either it has always been
			// sealed, or it was sealed away while this Reader was parked. Either
			// way its groups live in a sealed run.
			ref, ok := sealedCovering(r.root, r.next)
			if !ok {
				// A gap below OpenFrom that no sealed run covers: skip to the
				// open region and keep going.
				r.next = r.root.OpenFrom
				continue
			}
			sealed, err := r.sealed(ctx, ref)
			if err != nil {
				return GroupInfo{}, err
			}
			r.batch, r.idx, r.next, r.misses = sealed.Groups, 0, ref.LastDelta+1, 0
			continue
		}

		delta, err := r.delta(ctx, r.next)
		if err == nil {
			r.batch, r.idx, r.next, r.misses = delta.Groups, 0, r.next+1, 0
			continue
		}
		if errors.Is(err, ErrNotCommitted) {
			// The delta is absent. It may simply not be committed yet, or it may
			// have been sealed away. A root re-read settles which, but only
			// matters occasionally, so it is rationed.
			r.misses++
			if r.misses == 1 || r.misses%rootRecheckEvery == 0 {
				if e := r.Refresh(ctx); e != nil {
					return GroupInfo{}, e
				}
				// Fall through to io.EOF rather than re-probing: a refresh that
				// did not move OpenFrom past next would only add a redundant
				// probe, and one that did is picked up by the sealed branch on
				// the next call.
			}
			return GroupInfo{}, io.EOF
		}
		return GroupInfo{}, err
	}
}

// Position returns the most recently yielded group, for saving across a
// restart. Pass it to [Reader.SeekGroup] to resume strictly after it.
//
// Before any group has been yielded it is the zero [GroupRef], which SeekGroup
// reads as "from the start."
func (r *Reader) Position() GroupRef { return r.last }

// sealedCovering returns the sealed run holding a delta number, if any.
func sealedCovering(root rootManifest, delta uint64) (sealedRef, bool) {
	for _, ref := range root.Sealed {
		if delta >= ref.FirstDelta && delta <= ref.LastDelta {
			return ref, true
		}
	}

	return sealedRef{}, false
}

// walk iterates the track in commit order, fetching only the sealed runs that
// include accepts. A nil include fetches every run.
func (r *Reader) walk(ctx context.Context, include func(sealedRef) bool) iter.Seq2[GroupInfo, error] {
	return func(yield func(GroupInfo, error) bool) {
		root := r.rootManifest()

		for _, ref := range root.Sealed {
			if include != nil && !include(ref) {
				continue
			}

			sealed, err := r.sealed(ctx, ref)
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

		for n := root.OpenFrom; ; n++ {
			delta, err := r.delta(ctx, n)
			if errors.Is(err, ErrNotCommitted) {
				return
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
// with it — possible at all. For seeking within one track, use
// [Reader.SeekMedia], which is exact and immune to clock skew.
//
// SeekWallclock also positions the cursor at the group it returns, so a
// following [Reader.Next] loop plays forward from it.
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupInfo, error) {
	timescale := r.root.Timescale

	g, seg, idx, after, err := r.seek(ctx, r.root, unixNano,
		func(g GroupInfo) (int64, bool) { return g.Wallclock, g.hasWallclock() },
		func(g GroupInfo) (int64, bool) { return g.wallclockEnd(timescale) },
		func(ref sealedRef) (int64, bool) { return ref.WallclockStart, ref.WallclockStart != 0 },
	)
	if err != nil {
		return GroupInfo{}, err
	}
	r.position(seg, idx, after)
	return g, nil
}

// SeekMedia returns the group anchored at or before a media timestamp, in the
// track's timescale units.
//
// SeekMedia also positions the cursor at the group it returns — which is what a
// player wants: land on or before the target, then decode forward with
// [Reader.Next].
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupInfo, error) {
	g, seg, idx, after, err := r.seek(ctx, r.root, mediaTime,
		func(g GroupInfo) (int64, bool) { return g.MediaTime, true },
		func(g GroupInfo) (int64, bool) { return g.mediaEnd(), g.hasDuration() },
		func(ref sealedRef) (int64, bool) { return ref.MediaStart, true },
	)
	if err != nil {
		return GroupInfo{}, err
	}
	r.position(seg, idx, after)
	return g, nil
}

// seek returns the last group anchored at or before target, and where it lives:
// the segment slice it came from, its index within that segment, and the delta
// number to resume at once the segment is drained. [Reader.SeekMedia] and
// [Reader.SeekWallclock] use the location to position the cursor without a
// second fetch.
//
// It resolves against anchors rather than testing containment, which is both
// what a player wants — land on or before the target and decode forward — and
// what keeps seeking correct when a producer supplies no duration. Duration is
// consulted only at the end, to reject a target that falls past a known end.
//
// The search runs newest-first. The open region is examined before the sealed
// history because it holds the newest data, and the sealed runs are then walked
// in reverse until one begins at or before the target — so a seek fetches at
// most one sealed manifest rather than one per run, however long the track has
// been recording. Walking backwards also settles what an epoch reset makes
// ambiguous: when the same media timestamp exists in several epochs, the most
// recent one wins.
//
// The open region is bounded by the seal threshold, so scanning it is bounded
// too. seek takes the root explicitly so that resolving and positioning run
// against the same snapshot.
func (r *Reader) seek(
	ctx context.Context,
	root rootManifest,
	target int64,
	anchor func(GroupInfo) (int64, bool),
	end func(GroupInfo) (int64, bool),
	start func(sealedRef) (int64, bool),
) (GroupInfo, []GroupInfo, int, uint64, error) {
	var (
		best     GroupInfo
		bestAt   int64
		bestSeg  []GroupInfo
		bestIdx  int
		bestNext uint64
		found    bool
		consider = func(seg []GroupInfo, i int, after uint64) {
			group := seg[i]
			at, ok := anchor(group)
			if !ok || at > target {
				return
			}
			if !found || at >= bestAt {
				best, bestAt, bestSeg, bestIdx, bestNext, found = group, at, seg, i, after, true
			}
		}
	)

	for n := root.OpenFrom; ; n++ {
		delta, err := r.delta(ctx, n)
		if errors.Is(err, ErrNotCommitted) {
			break
		}
		if err != nil {
			return GroupInfo{}, nil, 0, 0, err
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

		sealed, err := r.sealed(ctx, ref)
		if err != nil {
			return GroupInfo{}, nil, 0, 0, err
		}
		for j := range sealed.Groups {
			consider(sealed.Groups, j, ref.LastDelta+1)
		}
	}

	if !found {
		return GroupInfo{}, nil, 0, 0, fmt.Errorf("%w: nothing in %s is anchored at or before %d", ErrGroupNotFound, r.track, target)
	}
	if at, ok := end(best); ok && target >= at {
		return GroupInfo{}, nil, 0, 0, fmt.Errorf("%w: %d falls past the end of %s in %s", ErrGroupNotFound, target, best.GroupRef, r.track)
	}

	return best, bestSeg, bestIdx, bestNext, nil
}

// locateAfter finds the first committed group strictly after ref — the resume
// point for a [Reader] positioned past a recorded [GroupRef]. It returns the
// segment the group lives in, its index within it, and the delta number to
// resume at once that segment is drained. When nothing follows ref it returns a
// nil segment with next set to the tip, so the caller parks at the end.
func (r *Reader) locateAfter(ctx context.Context, root rootManifest, ref GroupRef) (seg []GroupInfo, idx int, next uint64, err error) {
	for _, sref := range root.Sealed {
		// A run whose last group does not exceed ref holds nothing after it.
		if !ref.Before(sref.Last) {
			continue
		}
		sealed, err := r.sealed(ctx, sref)
		if err != nil {
			return nil, 0, 0, err
		}
		for i, g := range sealed.Groups {
			if ref.Before(g.GroupRef) {
				return sealed.Groups, i, sref.LastDelta + 1, nil
			}
		}
	}

	for n := root.OpenFrom; ; n++ {
		delta, err := r.delta(ctx, n)
		if errors.Is(err, ErrNotCommitted) {
			// Nothing follows ref; park at the tip.
			return nil, 0, n, nil
		}
		if err != nil {
			return nil, 0, 0, err
		}
		for i, g := range delta.Groups {
			if ref.Before(g.GroupRef) {
				return delta.Groups, i, n + 1, nil
			}
		}
	}
}

func fetchRoot(ctx context.Context, objects store.Store, track TrackPath) (rootManifest, store.Version, error) {
	data, version, err := objects.Get(ctx, rootKey(track))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return rootManifest{}, store.NoVersion, fmt.Errorf("%w: %s", ErrTrackNotFound, track)
		}
		return rootManifest{}, store.NoVersion, fmt.Errorf("ledger: read root of %s: %w", track, err)
	}

	root, err := decodeManifest(data, func(m rootManifest) int { return m.Version })
	if err != nil {
		return rootManifest{}, store.NoVersion, err
	}
	if root.Track != track {
		return rootManifest{}, store.NoVersion, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, rootKey(track), root.Track, track)
	}

	return root, version, nil
}

func fetchHead(ctx context.Context, objects store.Store, track TrackPath) (head, store.Version, error) {
	data, version, err := objects.Get(ctx, headKey(track))
	if err != nil {
		return head{}, store.NoVersion, err
	}

	h, err := decodeManifest(data, func(v head) int { return v.Version })
	if err != nil {
		return head{}, store.NoVersion, err
	}

	return h, version, nil
}
