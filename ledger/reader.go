package ledger

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultPollInterval is how often [Reader.Follow] probes for the next delta.
//
// Object stores do not push, so following a track means polling. The interval
// is the visibility latency floor for a reader, which is why the ledger is not
// a live path: even at zero polling delay, a group cannot be committed until it
// is sealed.
const DefaultPollInterval = 500 * time.Millisecond

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
// Reader is safe for concurrent use.
type Reader struct {
	objects store.Store
	track   TrackPath

	mu          sync.RWMutex
	root        RootManifest
	rootVersion store.Version
}

// OpenReader loads a track's root manifest.
func (b *Bucket) OpenReader(ctx context.Context, track TrackPath) (*Reader, error) {
	if err := track.validate(); err != nil {
		return nil, err
	}

	root, version, err := fetchRoot(ctx, b.objects, track)
	if err != nil {
		return nil, err
	}

	return &Reader{objects: b.objects, track: track, root: root, rootVersion: version}, nil
}

// Track returns the path being read.
func (r *Reader) Track() TrackPath { return r.track }

// Root returns a copy of the cached root manifest.
func (r *Reader) Root() RootManifest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	root := r.root
	root.Sealed = append([]SealedRef(nil), r.root.Sealed...)

	return root
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

// Head returns the track's head pointer.
//
// Head is a discovery cache and may lag the true tip. Use it to start near the
// end of a track, never to decide that a track has ended.
func (r *Reader) Head(ctx context.Context) (Head, error) {
	head, _, err := fetchHead(ctx, r.objects, r.track)

	return head, err
}

// Delta returns the nth delta manifest, or ErrNotCommitted if it does not
// exist yet. Absence is the normal signal that a reader has caught up with the
// writer, not a failure.
func (r *Reader) delta(ctx context.Context, n uint64) (DeltaManifest, error) {
	data, _, err := r.objects.Get(ctx, deltaKey(r.track, n))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return DeltaManifest{}, fmt.Errorf("%w: %s delta %d", ErrNotCommitted, r.track, n)
		}
		return DeltaManifest{}, fmt.Errorf("ledger: read delta %d: %w", n, err)
	}

	return decodeManifest(data, func(d DeltaManifest) int { return d.Version })
}

// Sealed returns a sealed manifest by its reference in the root.
func (r *Reader) sealed(ctx context.Context, ref SealedRef) (SealedManifest, error) {
	data, _, err := r.objects.Get(ctx, ref.Key)
	if err != nil {
		return SealedManifest{}, fmt.Errorf("ledger: read sealed manifest %q: %w", ref.Key, err)
	}

	sealed, err := decodeManifest(data, func(m SealedManifest) int { return m.Version })
	if err != nil {
		return SealedManifest{}, err
	}

	// The manifest names its own track and range, so disagreement with the
	// reference means the object is not the one the root meant to point at.
	switch {
	case sealed.Track != r.track:
		return SealedManifest{}, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, ref.Key, sealed.Track, r.track)
	case sealed.FirstDelta != ref.FirstDelta || sealed.LastDelta != ref.LastDelta:
		return SealedManifest{}, fmt.Errorf("%w: %q covers deltas %d-%d, expected %d-%d",
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

// Groups iterates every group in the track in commit order. Iteration stops at
// the first error, and at the tip of the track.
//
// This reads the whole recording. Prefer [Reader.RangeWallclock],
// [Reader.RangeMedia] or [Reader.GroupsFrom] for anything narrower.
func (r *Reader) Groups(ctx context.Context) iter.Seq2[GroupInfo, error] {
	return r.walk(ctx, nil)
}

// GroupsFrom iterates from a group onward, in commit order, including the group
// itself. It is the resumption path for a consumer that recorded how far it got.
func (r *Reader) GroupsFrom(ctx context.Context, from GroupRef) iter.Seq2[GroupInfo, error] {
	return filterGroups(
		// A sealed run whose last group precedes the mark holds nothing wanted.
		r.walk(ctx, func(ref SealedRef) bool { return !ref.Last.Before(from) }),
		func(group GroupInfo) bool { return !group.GroupRef.Before(from) },
	)
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
		r.walk(ctx, func(ref SealedRef) bool { return ref.MediaStart < to && ref.MediaEnd > from }),
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

	timescale := r.Root().Timescale

	return filterGroups(
		r.walk(ctx, func(ref SealedRef) bool {
			// SealedRef.WallclockEnd is the last anchor rather than an end, so a group
			// sitting on it may still reach into the window. Only a run that
			// begins after the window can be ruled out. A run with no anchors
			// at all cannot be ruled out either.
			return ref.WallclockStart == 0 || ref.WallclockStart < to
		}),
		func(group GroupInfo) bool { return group.overlapsWallclock(from, to, timescale) },
	)
}

// Tip returns a cursor positioned after everything currently committed, for a
// follower that wants new groups only.
//
// It is derived from the head pointer, which may lag the true tip, so following
// from it can replay a few groups that were already committed. Delivery is at
// least once by design; a consumer that must not act twice should be idempotent
// or track [GroupRef] itself.
func (r *Reader) Tip(ctx context.Context) (Cursor, error) {
	head, _, err := fetchHead(ctx, r.objects, r.track)
	if errors.Is(err, store.ErrNotExist) {
		// No head yet means nothing has been committed, so the start of the
		// track already is the tip.
		return Cursor{}, nil
	}
	if err != nil {
		return Cursor{}, err
	}

	return Cursor{delta: head.Delta + 1}, nil
}

// Follow yields groups from a cursor onward, polling for new ones until ctx is
// cancelled. The zero Cursor starts at the beginning of the track, so Follow
// drains history first and then tails.
//
// This is the notification mechanism. Object stores have no push, so a follower
// probes forward by deterministic key and treats absence as "not yet" — no
// listing, and no ledger process in the loop. A deployment wanting lower latency
// layers a real notification channel on top; nothing here depends on one.
//
// Each [Update] carries the cursor that resumes immediately after it, so a
// consumer can persist its position at group granularity.
func (r *Reader) Follow(ctx context.Context, from Cursor, interval time.Duration) iter.Seq2[Update, error] {
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	return func(yield func(Update, error) bool) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		at := position{delta: from.delta, index: from.index}
		// misses counts consecutive empty polls, so the root is re-read when a
		// follower first stalls and only occasionally after that.
		misses := 0

		for {
			manifest, err := r.delta(ctx, at.delta)
			switch {
			case err == nil:
				for i, group := range manifest.Groups {
					if i < at.index {
						continue
					}
					update := Update{
						GroupInfo: group,
						Cursor:    Cursor{delta: at.delta, index: i + 1},
					}
					if !yield(update, nil) {
						return
					}
				}
				at, misses = position{delta: at.delta + 1}, 0
				// Do not wait before trying the next one: a backlog should
				// drain at full speed, and only a genuine miss should poll.
				continue

			case errors.Is(err, ErrNotCommitted):
				// A missing delta means one of two very different things: it
				// has not been committed yet, or it was sealed and reclaimed
				// while this follower was away. Waiting is right for the first
				// and hangs forever on the second.
				next, outcome, err := r.resume(ctx, at, misses, yield)
				if err != nil {
					yield(Update{}, err)
					return
				}
				switch outcome {
				case resumeStopped:
					return
				case resumeAdvanced:
					at, misses = next, 0
					continue
				}
				misses++

			default:
				yield(Update{}, err)
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}
}

// rootRecheckEvery is how many consecutive empty polls pass between re-reads of
// the root manifest while a follower is stalled.
//
// A stalled follower needs the root only to notice a seal that happened since
// it last read one, and a seal cannot happen before the delta it is waiting for
// is even committed. Re-reading on every tick therefore doubles the request
// count of an idle follower to learn nothing.
const rootRecheckEvery = 8

// position is where a follower has reached: a delta, and how many of that
// delta's groups it has already consumed.
type position struct {
	delta uint64
	index int
}

// resumeOutcome says what resume decided about a delta it could not read.
type resumeOutcome int

const (
	// resumeWaiting means the delta is genuinely not committed yet, so the
	// follower should poll.
	resumeWaiting resumeOutcome = iota
	// resumeAdvanced means the delta had been sealed away; its groups were
	// served from the sealed run and the position moved past it.
	resumeAdvanced
	// resumeStopped means the consumer broke out of the loop.
	resumeStopped
)

// resume reports where a follower should continue when a delta is absent.
//
// Sealing deletes the deltas it folds up, so a cursor taken before a seal names
// an object that no longer exists — and a follower resuming from a persisted
// cursor would otherwise wait for it forever. The groups are not lost, only
// moved, so they are served from the sealed run instead and the position jumps
// past it.
//
// The cached root usually settles this without a request: OpenReader read it,
// so a delta below OpenFrom is known sealed. Only a seal that happened since
// needs a re-read, which is why that is rationed by rootRecheckEvery rather
// than done on every tick.
//
// The cursor handed out during a replay points after the whole run rather than
// after each group: a sealed manifest does not record which delta each of its
// groups came from. A consumer that stops mid-run and resumes will see the run
// again, which is within the at-least-once contract Follow already carries.
func (r *Reader) resume(
	ctx context.Context,
	at position,
	misses int,
	yield func(Update, error) bool,
) (position, resumeOutcome, error) {
	root := r.Root()

	if at.delta >= root.OpenFrom && (misses == 0 || misses%rootRecheckEvery == 0) {
		if err := r.Refresh(ctx); err != nil {
			return at, resumeWaiting, err
		}
		root = r.Root()
	}

	if at.delta >= root.OpenFrom {
		// Still in the open region, so it really has not been committed yet.
		return at, resumeWaiting, nil
	}

	ref, ok := sealedCovering(root, at.delta)
	if !ok {
		// Below the open region but in no sealed run — nothing can serve it,
		// so resume at the first delta that still exists.
		return position{delta: root.OpenFrom}, resumeAdvanced, nil
	}

	sealed, err := r.sealed(ctx, ref)
	if err != nil {
		return at, resumeWaiting, err
	}

	cursor := Cursor{delta: ref.LastDelta + 1}
	for _, group := range sealed.Groups {
		if !yield(Update{GroupInfo: group, Cursor: cursor}, nil) {
			return at, resumeStopped, nil
		}
	}

	return position{delta: ref.LastDelta + 1}, resumeAdvanced, nil
}

// sealedCovering returns the sealed run holding a delta number, if any.
func sealedCovering(root RootManifest, delta uint64) (SealedRef, bool) {
	for _, ref := range root.Sealed {
		if delta >= ref.FirstDelta && delta <= ref.LastDelta {
			return ref, true
		}
	}

	return SealedRef{}, false
}

// walk iterates the track in commit order, fetching only the sealed runs that
// include accepts. A nil include fetches every run.
func (r *Reader) walk(ctx context.Context, include func(SealedRef) bool) iter.Seq2[GroupInfo, error] {
	return func(yield func(GroupInfo, error) bool) {
		root := r.Root()

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
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupInfo, error) {
	timescale := r.Root().Timescale

	return r.seek(ctx, unixNano,
		func(g GroupInfo) (int64, bool) { return g.Wallclock, g.hasWallclock() },
		func(g GroupInfo) (int64, bool) { return g.wallclockEnd(timescale) },
		func(ref SealedRef) (int64, bool) { return ref.WallclockStart, ref.WallclockStart != 0 },
	)
}

// SeekMedia returns the group anchored at or before a media timestamp, in the
// track's timescale units.
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupInfo, error) {
	return r.seek(ctx, mediaTime,
		func(g GroupInfo) (int64, bool) { return g.MediaTime, true },
		func(g GroupInfo) (int64, bool) { return g.mediaEnd(), g.hasDuration() },
		func(ref SealedRef) (int64, bool) { return ref.MediaStart, true },
	)
}

// seek returns the last group anchored at or before target.
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
// too.
func (r *Reader) seek(
	ctx context.Context,
	target int64,
	anchor func(GroupInfo) (int64, bool),
	end func(GroupInfo) (int64, bool),
	start func(SealedRef) (int64, bool),
) (GroupInfo, error) {
	root := r.Root()

	var (
		best     GroupInfo
		bestAt   int64
		found    bool
		consider = func(group GroupInfo) {
			at, ok := anchor(group)
			if !ok || at > target {
				return
			}
			if !found || at >= bestAt {
				best, bestAt, found = group, at, true
			}
		}
	)

	for n := root.OpenFrom; ; n++ {
		delta, err := r.delta(ctx, n)
		if errors.Is(err, ErrNotCommitted) {
			break
		}
		if err != nil {
			return GroupInfo{}, err
		}
		for _, group := range delta.Groups {
			consider(group)
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
			return GroupInfo{}, err
		}
		for _, group := range sealed.Groups {
			consider(group)
		}
	}

	if !found {
		return GroupInfo{}, fmt.Errorf("%w: nothing in %s is anchored at or before %d", ErrGroupNotFound, r.track, target)
	}
	if at, ok := end(best); ok && target >= at {
		return GroupInfo{}, fmt.Errorf("%w: %d falls past the end of %s in %s", ErrGroupNotFound, target, best.GroupRef, r.track)
	}

	return best, nil
}

func fetchRoot(ctx context.Context, objects store.Store, track TrackPath) (RootManifest, store.Version, error) {
	data, version, err := objects.Get(ctx, rootKey(track))
	if err != nil {
		if errors.Is(err, store.ErrNotExist) {
			return RootManifest{}, store.NoVersion, fmt.Errorf("%w: %s", ErrTrackNotFound, track)
		}
		return RootManifest{}, store.NoVersion, fmt.Errorf("ledger: read root of %s: %w", track, err)
	}

	root, err := decodeManifest(data, func(m RootManifest) int { return m.Version })
	if err != nil {
		return RootManifest{}, store.NoVersion, err
	}
	if root.Track != track {
		return RootManifest{}, store.NoVersion, fmt.Errorf("%w: %q holds track %s, expected %s",
			ErrManifestMismatch, rootKey(track), root.Track, track)
	}

	return root, version, nil
}

func fetchHead(ctx context.Context, objects store.Store, track TrackPath) (Head, store.Version, error) {
	data, version, err := objects.Get(ctx, headKey(track))
	if err != nil {
		return Head{}, store.NoVersion, err
	}

	head, err := decodeManifest(data, func(h Head) int { return h.Version })
	if err != nil {
		return Head{}, store.NoVersion, err
	}

	return head, version, nil
}
