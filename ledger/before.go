package ledger

import (
	"context"
	"errors"
	"slices"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Before returns up to n groups committed before the group id, oldest first.
// The zero id stands after every group, so Before(ctx, 0, n) returns the
// newest n. Paging backwards through a track passes the first group of one
// page as the id of the next.
//
// It reads backwards from the newest delta of each epoch and skips every sealed
// run that starts at or after id, so a page costs about as many requests as it
// has groups that are not yet sealed, plus one per sealed run it reaches into.
// It does not move the streaming cursor.
func (r *Reader) Before(ctx context.Context, id GroupID, n int) ([]GroupInfo, error) {
	if n <= 0 {
		return nil, nil
	}
	epoch := r.latest
	if id != 0 && id.Epoch() < epoch {
		epoch = id.Epoch()
	}

	// Newest first; reversed into commit order on return.
	var groups []GroupInfo
	take := func(batch []GroupInfo) bool {
		for _, g := range slices.Backward(batch) {
			if id != 0 && g.ID.Compare(id) >= 0 {
				continue
			}
			groups = append(groups, g)
			if len(groups) == n {
				return true
			}
		}
		return false
	}

	for ; epoch >= 1; epoch-- {
		logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, epoch)
		if err != nil {
			return nil, err
		}
		last, err := r.lastDelta(ctx, epoch, logRoot.OpenFrom)
		if err != nil {
			return nil, err
		}
		for d := last; d >= logRoot.OpenFrom && d != noDelta; d-- {
			delta, err := r.delta(ctx, epoch, d)
			if errors.Is(err, ErrNotCommitted) {
				// A seal reclaimed it after the log root was read; its groups
				// are in the newest sealed run, which is read next.
				continue
			}
			if err != nil {
				return nil, err
			}
			if take(delta.Groups) {
				slices.Reverse(groups)
				return groups, nil
			}
		}
		for _, ref := range slices.Backward(logRoot.Sealed) {
			if id != 0 && ref.First.Compare(id) >= 0 {
				continue
			}
			sealed, err := r.sealed(ctx, epoch, ref)
			if err != nil {
				return nil, err
			}
			if take(sealed.Groups) {
				slices.Reverse(groups)
				return groups, nil
			}
		}
	}
	slices.Reverse(groups)
	return groups, nil
}

// noDelta is what lastDelta returns for an epoch with no open delta.
const noDelta = ^uint64(0)

// lastDelta returns the number of the epoch's newest committed delta, or
// noDelta when the open region holds none. The head pointer may lag the tip,
// so the deltas after it are probed until one is not committed.
func (r *Reader) lastDelta(ctx context.Context, epoch, openFrom uint64) (uint64, error) {
	h, _, err := fetchHead(ctx, r.objects, r.path, epoch)
	last := noDelta
	switch {
	case errors.Is(err, store.ErrNotExist):
	case err != nil:
		return 0, err
	case h.Delta >= openFrom:
		last = h.Delta
	}
	next := openFrom
	if last != noDelta {
		next = last + 1
	}
	for ; ; next++ {
		_, _, err := r.objects.Get(ctx, deltaKey(r.path, epoch, next))
		if errors.Is(err, store.ErrNotExist) {
			return last, nil
		}
		if err != nil {
			return 0, err
		}
		last = next
	}
}
