package ledger

import (
	"context"
	"errors"
	"io"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultPollInterval is a suggested interval for a tailing [Scanner] poll
// loop.
//
// Object stores do not push, so following a track means polling, and the
// interval is the visibility latency floor for a tailing reader — which is one
// reason the ledger is not a live path. A [Scanner] does not poll itself;
// [Scanner.Next] is non-blocking and returns io.EOF at the tip, leaving the
// wait to the caller. This constant is the value the cmd and most callers use.
const DefaultPollInterval = 500_000_000 // 500ms, as a count of nanoseconds

// rootRecheckEvery is how many consecutive empty polls pass between re-reads of
// the root manifest while a Scanner is stalled at the tip.
//
// A stalled Scanner needs the root only to notice a seal that happened since it
// last read one, and a seal cannot happen before the delta it is waiting for is
// even committed. Re-reading on every poll therefore learns nothing and wastes
// a request, so it is rationed.
const rootRecheckEvery = 8

// Scanner is a positioned cursor over a track's groups in commit order, in the
// shape of [bufio.Scanner] and [database/sql.Rows]: position it with a Seek
// method, then drain groups with [Scanner.Next], which returns [io.EOF] at the
// current tip.
//
// A Scanner is the read path for streaming — seek and play forward, or tail new
// groups — where the [Reader]'s range methods answer a bounded snapshot. It
// holds its own root snapshot, independent of the Reader and of any other
// Scanner, so concurrent consumers each take their own.
//
// Tailing is a poll loop the caller owns, because object stores do not push and
// the right latency strategy (interval, signal, hybrid) is a deployment
// decision rather than the library's:
//
//	sc, _ := reader.NewScanner(ctx)
//	sc.SeekTip(ctx)
//	ticker := time.NewTicker(ledger.DefaultPollInterval)
//	defer ticker.Stop()
//	for {
//		group, err := sc.Next(ctx)
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
// io.EOF from Next is not terminal: it means "caught up for now," and the next
// call re-probes. A real error is terminal.
//
// A Scanner is not safe for concurrent use.
type Scanner struct {
	r    *Reader
	root rootManifest

	// next is the delta number the Scanner will consume next. batch holds the
	// groups of the segment that delta belongs to (a sealed run or an open
	// delta), with idx the next unread group within it.
	next   uint64
	idx    int
	batch  []GroupInfo
	misses int

	// last is the most recently yielded group, for Position. The zero value
	// means "nothing yielded yet", which SeekGroup reads as "from the start".
	last GroupRef
}

// SeekStart positions the Scanner at the first group, so a following [Scanner.Next]
// loop drains the whole recording before tailing.
func (s *Scanner) SeekStart() {
	s.next, s.idx, s.batch = 0, 0, nil
	s.last, s.misses = GroupRef{}, 0
}

// SeekTip positions the Scanner after everything currently committed, so only
// groups committed after the call arrive. The position is derived from the head
// pointer, which may lag the true tip, so a tail from it can replay a few
// groups that were already committed — delivery is at least once by design.
func (s *Scanner) SeekTip(ctx context.Context) error {
	switch h, _, err := fetchHead(ctx, s.r.objects, s.r.track); {
	case errors.Is(err, store.ErrNotExist):
		// Nothing committed yet: the start of the track is already the tip.
		s.next = 0
	case err != nil:
		return err
	default:
		s.next = h.Delta + 1
	}

	s.idx, s.batch = 0, nil
	s.last, s.misses = GroupRef{}, 0
	return nil
}

// SeekMedia positions the Scanner at the group anchored at or before the media
// time, in the track's timescale units, and returns that group. A following
// [Scanner.Next] loop plays forward from it — which is what a player wants:
// land on or before the target, then decode forward.
func (s *Scanner) SeekMedia(ctx context.Context, mediaTime int64) (GroupInfo, error) {
	g, seg, idx, after, err := s.r.seek(ctx, s.root, mediaTime,
		func(g GroupInfo) (int64, bool) { return g.MediaTime, true },
		func(g GroupInfo) (int64, bool) { return g.mediaEnd(), g.hasDuration() },
		func(ref sealedRef) (int64, bool) { return ref.MediaStart, true },
	)
	if err != nil {
		return GroupInfo{}, err
	}
	s.position(seg, idx, after)
	return g, nil
}

// SeekWallclock positions the Scanner at the group anchored at or before a
// Unix-nanosecond instant, skipping groups that carry no wallclock anchor, and
// returns that group. See [Scanner.SeekMedia].
func (s *Scanner) SeekWallclock(ctx context.Context, unixNano int64) (GroupInfo, error) {
	timescale := s.root.Timescale

	g, seg, idx, after, err := s.r.seek(ctx, s.root, unixNano,
		func(g GroupInfo) (int64, bool) { return g.Wallclock, g.hasWallclock() },
		func(g GroupInfo) (int64, bool) { return g.wallclockEnd(timescale) },
		func(ref sealedRef) (int64, bool) { return ref.WallclockStart, ref.WallclockStart != 0 },
	)
	if err != nil {
		return GroupInfo{}, err
	}
	s.position(seg, idx, after)
	return g, nil
}

// SeekGroup positions the Scanner strictly after ref, so a following
// [Scanner.Next] loop resumes without re-yielding the group ref names. Pair it
// with [Scanner.Position] to resume across a restart.
func (s *Scanner) SeekGroup(ctx context.Context, ref GroupRef) error {
	seg, idx, after, err := s.r.locateAfter(ctx, s.root, ref)
	if err != nil {
		return err
	}
	// Whether or not a following group exists, `after` is the right place to
	// resume: a real segment's tail, or the tip when ref is past everything.
	s.batch, s.idx, s.next = seg, idx, after
	s.last, s.misses = GroupRef{}, 0
	return nil
}

// position sets the cursor to a resolved segment. Shared by the time-based
// seeks.
func (s *Scanner) position(seg []GroupInfo, idx int, after uint64) {
	s.batch, s.idx, s.next = seg, idx, after
	s.last, s.misses = GroupRef{}, 0
}

// Next advances to and returns the next group in commit order. It returns
// [io.EOF] when the Scanner has reached the current tip — caught up, for now —
// and a real error only when the store failed.
//
// io.EOF is not terminal: a later group may have been committed, so the next
// call re-probes. It mutates nothing but the empty-poll counter, so a caller
// may poll indefinitely:
//
//	for {
//		group, err := sc.Next(ctx)
//		if errors.Is(err, io.EOF) {
//			// wait, then call Next again
//		}
//	}
//
// A delta that has been sealed and reclaimed since the Scanner last read the
// root is still served: its groups moved into a sealed run, and Next notices on
// its rationed root re-read rather than hanging on the deleted delta.
func (s *Scanner) Next(ctx context.Context) (GroupInfo, error) {
	for {
		if s.idx < len(s.batch) {
			group := s.batch[s.idx]
			s.idx++
			s.last = group.GroupRef
			return group, nil
		}

		// batch exhausted: load the next segment.

		if s.next < s.root.OpenFrom {
			// The next delta is below the open region: either it has always been
			// sealed, or it was sealed away while this Scanner was parked. Either
			// way its groups live in a sealed run.
			ref, ok := sealedCovering(s.root, s.next)
			if !ok {
				// A gap below OpenFrom that no sealed run covers: skip to the
				// open region and keep going.
				s.next = s.root.OpenFrom
				continue
			}
			sealed, err := s.r.sealed(ctx, ref)
			if err != nil {
				return GroupInfo{}, err
			}
			s.batch, s.idx, s.next, s.misses = sealed.Groups, 0, ref.LastDelta+1, 0
			continue
		}

		delta, err := s.r.delta(ctx, s.next)
		if err == nil {
			s.batch, s.idx, s.next, s.misses = delta.Groups, 0, s.next+1, 0
			continue
		}
		if errors.Is(err, ErrNotCommitted) {
			// The delta is absent. It may simply not be committed yet, or it may
			// have been sealed away. A root re-read settles which, but only
			// matters occasionally, so it is rationed.
			s.misses++
			if s.misses == 1 || s.misses%rootRecheckEvery == 0 {
				if e := s.refresh(ctx); e != nil {
					return GroupInfo{}, e
				}
				// Fall through to io.EOF rather than re-probing: a refresh that
				// did not move OpenFrom past next would only add a redundant
				// probe, and one that did is picked up by the sealed branch on
				// the next call. This keeps a stalled Scanner at one probe per
				// poll.
			}
			return GroupInfo{}, io.EOF
		}
		return GroupInfo{}, err
	}
}

// Position returns the most recently yielded group, for saving across a
// restart. Pass it to [Scanner.SeekGroup] to resume strictly after it.
//
// Before any group has been yielded it is the zero [GroupRef], which
// SeekGroup reads as "from the start."
func (s *Scanner) Position() GroupRef { return s.last }

// Refresh re-reads the root manifest, picking up history sealed since the
// Scanner was constructed. Seeking into history that may have been rotated
// benefits from it; tailing the open region does not, since Next re-reads the
// root on its own schedule.
//
// Refresh is independent of [Reader.Refresh]: a Scanner holds its own root
// snapshot so that one Scanner's refresh never moves another's.
func (s *Scanner) Refresh(ctx context.Context) error {
	return s.refresh(ctx)
}

// refresh re-reads the root into the Scanner's own snapshot. It uses fetchRoot
// directly rather than Reader.Refresh, which would mutate the shared Reader and
// every other Scanner built from it.
func (s *Scanner) refresh(ctx context.Context) error {
	root, _, err := fetchRoot(ctx, s.r.objects, s.r.track)
	if err != nil {
		return err
	}
	s.root = root
	return nil
}
