package ledger

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Reader reads a track. It needs nothing but object-store access — no ledger
// process has to be running for a Reader to seek or replay.
//
// # Iteration and errors
//
// The iterating methods yield an error alongside each value because every step
// is a request to the object store, so a failure part-way through is expected
// rather than exceptional.
//
// That error is terminal and yielded at most once: iteration stops immediately
// after it, and no further value follows. A loop may therefore return or break
// on a non-nil error without needing to drain the rest.
//
//	for group, err := range reader.RangeWallclock(ctx, from, to) {
//		if err != nil {
//			return err
//		}
//		// ...
//	}
//
// Reader is stateless and safe for concurrent use. The range methods answer a
// bounded query as a snapshot; for open-ended streaming — seek and play
// forward, or tail new groups — take a [Scanner] with [Reader.NewScanner].
//
// Reader is safe for concurrent use.
type Reader struct {
	objects store.Store
	track   TrackPath

	mu          sync.RWMutex
	root        rootManifest
	rootVersion store.Version
}

// Reader opens the track for reading. It loads the root manifest and nothing
// else: reading needs no recovery, so a reader joins cheaply.
func (t *Track) Reader(ctx context.Context) (*Reader, error) {
	root, version, err := fetchRoot(ctx, t.store, t.path)
	if err != nil {
		return nil, err
	}

	return &Reader{objects: t.store, track: t.path, root: root, rootVersion: version}, nil
}

// Track returns the path being read.
func (r *Reader) Track() TrackPath { return r.track }

// rootManifest returns a defensive copy of the cached root. Internal callers
// need the full root — its sealed index and open region — to seek and walk; the
// projection [Reader.Root] returns hides all of that.
func (r *Reader) rootManifest() rootManifest {
	r.mu.RLock()
	defer r.mu.RUnlock()

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

// Refresh re-reads the root manifest, picking up manifests sealed since the
// Reader was opened. Tailing does not require it — the open region is
// discovered by probing — but seeking into history does.
func (r *Reader) Refresh(ctx context.Context) error {
	root, version, err := fetchRoot(ctx, r.objects, r.track)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.root, r.rootVersion = root, version

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

// NewScanner returns a positioned cursor over the track's groups, in commit
// order, for seek-and-stream and tailing. See [Scanner].
//
// A Scanner holds its own root snapshot and is not safe for concurrent use;
// concurrent consumers each take their own.
func (r *Reader) NewScanner(ctx context.Context) (*Scanner, error) {
	root, _, err := fetchRoot(ctx, r.objects, r.track)
	if err != nil {
		return nil, err
	}
	return &Scanner{r: r, root: root}, nil
}

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
// For streaming forward from the result, use a [Scanner] instead.
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupInfo, error) {
	timescale := r.rootManifest().Timescale

	g, _, _, _, err := r.seek(ctx, r.rootManifest(), unixNano,
		func(g GroupInfo) (int64, bool) { return g.Wallclock, g.hasWallclock() },
		func(g GroupInfo) (int64, bool) { return g.wallclockEnd(timescale) },
		func(ref sealedRef) (int64, bool) { return ref.WallclockStart, ref.WallclockStart != 0 },
	)
	return g, err
}

// SeekMedia returns the group anchored at or before a media timestamp, in the
// track's timescale units.
//
// For streaming forward from the result, use a [Scanner] instead.
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupInfo, error) {
	g, _, _, _, err := r.seek(ctx, r.rootManifest(), mediaTime,
		func(g GroupInfo) (int64, bool) { return g.MediaTime, true },
		func(g GroupInfo) (int64, bool) { return g.mediaEnd(), g.hasDuration() },
		func(ref sealedRef) (int64, bool) { return ref.MediaStart, true },
	)
	return g, err
}

// seek returns the last group anchored at or before target, and where it lives:
// the segment slice it came from, its index within that segment, and the delta
// number to resume at once the segment is drained. A [Scanner] uses the
// location to position itself without a second fetch; [Reader.SeekMedia] and
// [Reader.SeekWallclock] discard it.
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
// too. seek runs against the root it is given, so a [Scanner] passes its own
// snapshot and avoids a resolve-versus-position race.
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
// point for a [Scanner] positioned past a recorded [GroupRef]. It returns the
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
