package ledger

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"time"

	"github.com/okdaichi/qumo-ledger/objectstore"
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
// Reader is safe for concurrent use.
type Reader struct {
	store objectstore.Store
	track TrackPath

	mu          sync.RWMutex
	root        RootManifest
	rootVersion objectstore.Version
}

// OpenReader loads a track's root manifest.
func OpenReader(ctx context.Context, store objectstore.Store, track TrackPath) (*Reader, error) {
	if err := track.Validate(); err != nil {
		return nil, err
	}

	root, version, err := fetchRoot(ctx, store, track)
	if err != nil {
		return nil, err
	}

	return &Reader{store: store, track: track, root: root, rootVersion: version}, nil
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
	root, version, err := fetchRoot(ctx, r.store, r.track)
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
	head, _, err := fetchHead(ctx, r.store, r.track)

	return head, err
}

// Delta returns the nth delta manifest, or ErrNotCommitted if it does not
// exist yet. Absence is the normal signal that a reader has caught up with the
// writer, not a failure.
func (r *Reader) Delta(ctx context.Context, n uint64) (DeltaManifest, error) {
	data, _, err := r.store.Get(ctx, deltaKey(r.track, n))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotExist) {
			return DeltaManifest{}, fmt.Errorf("%w: %s delta %d", ErrNotCommitted, r.track, n)
		}
		return DeltaManifest{}, fmt.Errorf("ledger: read delta %d: %w", n, err)
	}

	return decodeManifest(data, func(d DeltaManifest) int { return d.Version })
}

// Sealed returns a sealed manifest by its reference in the root.
func (r *Reader) Sealed(ctx context.Context, ref SealedRef) (SealedManifest, error) {
	data, _, err := r.store.Get(ctx, ref.Key)
	if err != nil {
		return SealedManifest{}, fmt.Errorf("ledger: read sealed manifest %q: %w", ref.Key, err)
	}

	return decodeManifest(data, func(m SealedManifest) int { return m.Version })
}

// ReadGroup fetches a group's payload.
//
// It reads [GroupMeta.Object] rather than deriving a key, because producer
// sequences are gappy and a derived key can name a group that was dropped.
func (r *Reader) ReadGroup(ctx context.Context, meta GroupMeta) ([]byte, error) {
	data, _, err := r.store.Get(ctx, meta.Object)
	if err != nil {
		return nil, fmt.Errorf("ledger: read group %s: %w", meta.GroupRef, err)
	}

	return data, nil
}

// Groups iterates every group in the track in commit order, walking the sealed
// manifests first and then probing the open region. Iteration stops at the
// first error, and at the tip of the track.
func (r *Reader) Groups(ctx context.Context) iter.Seq2[GroupMeta, error] {
	return func(yield func(GroupMeta, error) bool) {
		root := r.Root()

		for _, ref := range root.Sealed {
			sealed, err := r.Sealed(ctx, ref)
			if err != nil {
				yield(GroupMeta{}, err)
				return
			}
			for _, group := range sealed.Groups {
				if !yield(group, nil) {
					return
				}
			}
		}

		for n := root.OpenFrom; ; n++ {
			delta, err := r.Delta(ctx, n)
			if errors.Is(err, ErrNotCommitted) {
				return
			}
			if err != nil {
				yield(GroupMeta{}, err)
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

// Follow yields delta manifests from n onward, polling for each next delta
// until ctx is cancelled.
//
// This is the notification mechanism. Object stores have no push, so a follower
// probes forward by deterministic key and treats absence as "not yet" — no
// listing, and no ledger process in the loop. A deployment that wants lower
// latency layers a real notification channel on top; nothing here depends on
// one existing.
func (r *Reader) Follow(ctx context.Context, from uint64, interval time.Duration) iter.Seq2[DeltaManifest, error] {
	if interval <= 0 {
		interval = DefaultPollInterval
	}

	return func(yield func(DeltaManifest, error) bool) {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for n := from; ; {
			delta, err := r.Delta(ctx, n)
			switch {
			case err == nil:
				if !yield(delta, nil) {
					return
				}
				n++
				// Do not wait before trying the next one: a backlog should
				// drain at full speed, and only a genuine miss should poll.
				continue
			case errors.Is(err, ErrNotCommitted):
			default:
				yield(DeltaManifest{}, err)
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

// SeekWallclock returns the group anchored at or before a Unix-nanosecond
// instant, skipping groups that carry no wallclock anchor.
//
// Wallclock is the cross-track key: seeking two tracks to the same instant is
// what makes correlated replay — video alongside the sensor readings recorded
// with it — possible at all. For seeking within one track, use
// [Reader.SeekMedia], which is exact and immune to clock skew.
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupMeta, error) {
	timescale := r.Root().Timescale

	return r.seek(ctx, unixNano,
		func(g GroupMeta) (int64, bool) { return g.W0, g.HasWallclock() },
		func(g GroupMeta) (int64, bool) { return g.wallclockEnd(timescale) },
		func(ref SealedRef) (int64, bool) { return ref.W0, ref.W0 != 0 },
	)
}

// SeekMedia returns the group anchored at or before a media timestamp, in the
// track's timescale units.
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupMeta, error) {
	return r.seek(ctx, mediaTime,
		func(g GroupMeta) (int64, bool) { return g.T0, true },
		func(g GroupMeta) (int64, bool) { return g.MediaEnd(), g.HasDuration() },
		func(ref SealedRef) (int64, bool) { return ref.T0, true },
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
	anchor func(GroupMeta) (int64, bool),
	end func(GroupMeta) (int64, bool),
	start func(SealedRef) (int64, bool),
) (GroupMeta, error) {
	root := r.Root()

	var (
		best     GroupMeta
		bestAt   int64
		found    bool
		consider = func(group GroupMeta) {
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
		delta, err := r.Delta(ctx, n)
		if errors.Is(err, ErrNotCommitted) {
			break
		}
		if err != nil {
			return GroupMeta{}, err
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

		sealed, err := r.Sealed(ctx, ref)
		if err != nil {
			return GroupMeta{}, err
		}
		for _, group := range sealed.Groups {
			consider(group)
		}
	}

	if !found {
		return GroupMeta{}, fmt.Errorf("%w: nothing in %s is anchored at or before %d", ErrNoGroupFound, r.track, target)
	}
	if at, ok := end(best); ok && target >= at {
		return GroupMeta{}, fmt.Errorf("%w: %d falls past the end of %s in %s", ErrNoGroupFound, target, best.GroupRef, r.track)
	}

	return best, nil
}

func fetchRoot(ctx context.Context, store objectstore.Store, track TrackPath) (RootManifest, objectstore.Version, error) {
	data, version, err := store.Get(ctx, rootKey(track))
	if err != nil {
		if errors.Is(err, objectstore.ErrNotExist) {
			return RootManifest{}, objectstore.NoVersion, fmt.Errorf("%w: %s", ErrTrackNotFound, track)
		}
		return RootManifest{}, objectstore.NoVersion, fmt.Errorf("ledger: read root of %s: %w", track, err)
	}

	root, err := decodeManifest(data, func(m RootManifest) int { return m.Version })
	if err != nil {
		return RootManifest{}, objectstore.NoVersion, err
	}

	return root, version, nil
}

func fetchHead(ctx context.Context, store objectstore.Store, track TrackPath) (Head, objectstore.Version, error) {
	data, version, err := store.Get(ctx, headKey(track))
	if err != nil {
		return Head{}, objectstore.NoVersion, err
	}

	head, err := decodeManifest(data, func(h Head) int { return h.Version })
	if err != nil {
		return Head{}, objectstore.NoVersion, err
	}

	return head, version, nil
}
