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

// SeekWallclock returns the group covering a Unix-nanosecond instant.
//
// Wallclock is the cross-track key: seeking two tracks to the same instant is
// what makes correlated replay — video alongside the sensor readings recorded
// with it — possible at all. For frame-accurate seeking inside a single track,
// use [Reader.SeekMedia].
func (r *Reader) SeekWallclock(ctx context.Context, unixNano int64) (GroupMeta, error) {
	return r.seek(ctx, func(ref SealedRef) bool {
		return ref.Covers(unixNano)
	}, func(group GroupMeta) bool {
		return group.ContainsWallclock(unixNano)
	})
}

// SeekMedia returns the group covering a media timestamp, in the track's
// timescale units. Media time is exact and skew-free but relative to this
// track's origin, so it cannot be compared across producers.
func (r *Reader) SeekMedia(ctx context.Context, mediaTime int64) (GroupMeta, error) {
	return r.seek(ctx, func(ref SealedRef) bool {
		return mediaTime >= ref.T0 && mediaTime < ref.T1
	}, func(group GroupMeta) bool {
		return group.Contains(mediaTime)
	})
}

// seek scans sealed manifests whose summary may cover the target before
// falling back to the open region. The summaries are what keep this from
// degrading into a scan of the entire history.
func (r *Reader) seek(ctx context.Context, covers func(SealedRef) bool, matches func(GroupMeta) bool) (GroupMeta, error) {
	root := r.Root()

	for _, ref := range root.Sealed {
		if !covers(ref) {
			continue
		}

		sealed, err := r.Sealed(ctx, ref)
		if err != nil {
			return GroupMeta{}, err
		}
		for _, group := range sealed.Groups {
			if matches(group) {
				return group, nil
			}
		}
	}

	for n := root.OpenFrom; ; n++ {
		delta, err := r.Delta(ctx, n)
		if errors.Is(err, ErrNotCommitted) {
			break
		}
		if err != nil {
			return GroupMeta{}, err
		}
		for _, group := range delta.Groups {
			if matches(group) {
				return group, nil
			}
		}
	}

	return GroupMeta{}, fmt.Errorf("ledger: no group covers the requested instant in %s", r.track)
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
