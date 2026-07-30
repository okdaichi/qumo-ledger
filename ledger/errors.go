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

	// ErrInvalidGroup reports a GroupMeta that violates an invariant — an
	// inverted time range, a zero sequence, or a missing epoch.
	ErrInvalidGroup = errors.New("ledger: invalid group")

	// ErrUnsupportedVersion reports a manifest written by an incompatible
	// version of this package.
	ErrUnsupportedVersion = errors.New("ledger: unsupported manifest version")

	// ErrNotCommitted reports that a delta has not been written yet. It is the
	// expected outcome of probing past the tip of a track and is how a tailing
	// reader learns to wait rather than an error condition.
	ErrNotCommitted = errors.New("ledger: delta not committed")
)
