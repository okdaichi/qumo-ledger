package ledger

import "errors"

var (
	// ErrTrackExists reports that a track already has a root manifest.
	ErrTrackExists = errors.New("ledger: track already exists")

	// ErrTrackNotFound reports that no root manifest exists for a track.
	ErrTrackNotFound = errors.New("ledger: track not found")

	// ErrGroupExists reports that a group object is already stored for an
	// (Epoch, Sequence) pair. Because group objects are immutable, this is how
	// a duplicate append is refused rather than silently overwriting data.
	ErrGroupExists = errors.New("ledger: group already exists")

	// ErrInvalidTrackPath reports a malformed track path.
	ErrInvalidTrackPath = errors.New("ledger: invalid track path")

	// ErrInvalidGroup reports a GroupMeta that violates an invariant on its
	// own — a negative duration, size, or wallclock, or a missing epoch.
	ErrInvalidGroup = errors.New("ledger: invalid group")

	// ErrGroupOutOfOrder reports a group that contradicts the one committed
	// before it: it starts before its predecessor ended, or its epoch runs
	// behind the track's. Groups are serial within an epoch, so this is a
	// contradiction rather than a gap — gaps are legal and expected.
	ErrGroupOutOfOrder = errors.New("ledger: group out of order")

	// ErrUnsupportedVersion reports a manifest written by an incompatible
	// version of this package.
	ErrUnsupportedVersion = errors.New("ledger: unsupported manifest version")

	// ErrNoGroupFound reports that a seek found no group at or before the
	// requested instant, or that the instant falls past the end of the last
	// group whose duration is known.
	ErrNoGroupFound = errors.New("ledger: no group found")

	// ErrNotCommitted reports that a delta has not been written yet. It is the
	// expected outcome of probing past the tip of a track and is how a tailing
	// reader learns to wait rather than an error condition.
	ErrNotCommitted = errors.New("ledger: delta not committed")
)
