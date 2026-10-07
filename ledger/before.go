package ledger

import (
	"context"
	"errors"
	"fmt"
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
		start := len(groups)
		for attempt := 1; ; attempt++ {
			full, resealed, err := r.beforeIn(ctx, epoch, id, take)
			if err != nil {
				return nil, err
			}
			if full {
				slices.Reverse(groups)
				return groups, nil
			}
			if !resealed {
				break
			}
			if attempt == beforeAttempts {
				return nil, fmt.Errorf("ledger: epoch %d of %s was sealed %d times while it was read", epoch, r.path, attempt)
			}
			// The epoch's groups moved to a sealed run the log root read did
			// not list: read the epoch again from a fresh log root.
			groups = groups[:start]
		}
	}
	slices.Reverse(groups)
	return groups, nil
}

// beforeAttempts is how many times Before reads an epoch that a seal keeps
// changing under it before giving up.
const beforeAttempts = 3

// beforeIn hands take the groups of one epoch, newest first, from its newest
// delta back through its sealed runs, skipping runs that start at or after id.
// It stops when take reports it is full. It reports resealed when a delta the
// log root still counted as open was gone: a seal moved its groups into a
// sealed run this log root does not list, so the epoch must be read again.
func (r *Reader) beforeIn(ctx context.Context, epoch uint64, id GroupID, take func([]GroupInfo) bool) (full, resealed bool, err error) {
	logRoot, _, err := fetchEpochLog(ctx, r.objects, r.path, epoch)
	if err != nil {
		return false, false, err
	}
	last, err := r.lastDelta(ctx, epoch, logRoot.OpenFrom)
	if err != nil {
		return false, false, err
	}
	for d := last; d >= logRoot.OpenFrom && d != noDelta; d-- {
		delta, err := r.delta(ctx, epoch, d)
		if errors.Is(err, ErrNotCommitted) {
			return false, true, nil
		}
		if err != nil {
			return false, false, err
		}
		if take(delta.Groups) {
			return true, false, nil
		}
	}
	for _, ref := range slices.Backward(logRoot.Sealed) {
		if id != 0 && ref.First.Compare(id) >= 0 {
			continue
		}
		sealed, err := r.sealed(ctx, epoch, ref)
		if err != nil {
			return false, false, err
		}
		if take(sealed.Groups) {
			return true, false, nil
		}
	}
	return false, false, nil
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
