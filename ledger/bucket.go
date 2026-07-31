package ledger

import (
	"errors"
	"log/slog"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// ErrNoStore reports a Bucket used before its Store was set.
var ErrNoStore = errors.New("ledger: Bucket.Store is not set")

// Bucket holds tracks in an object store and is the entry point to everything
// else: a [Writer] or [Reader] is opened from it rather than constructed
// against a bare store.
//
// Binding the store once is what lets settings be shared. A logger or a clock
// belongs to a deployment rather than to one track, so passing them at every
// call would mean repeating them and, worse, leaving no way to give them to a
// reader at all.
//
// A Bucket sits above whatever the store is backed by, not on it: one may wrap
// a prefix inside an S3 bucket, a local directory, or memory, and several may
// share one backend. It holds no resources and needs no closing.
//
//	bucket := &ledger.Bucket{Store: objects, Logger: logger}
//
// [Bucket.Store] must be set; every other field means a documented default when
// left zero. A Bucket is safe for concurrent use, but must not be modified once
// a Writer or Reader has been opened from it.
type Bucket struct {
	// Store is the object store the tracks live in. It has no default and
	// must be set.
	Store store.Store

	// SealThreshold is how many bytes of open manifest accumulate before the
	// open region is rotated into a sealed manifest. Zero means
	// [DefaultSealThreshold].
	SealThreshold int64

	// Clock supplies wallclock time, for tests and for producers with their
	// own notion of ingest time. Nil means [time.Now].
	Clock func() time.Time

	// Logger receives events that do not affect correctness, such as a failed
	// head update. Nil means [slog.Default].
	Logger *slog.Logger
}

// sealThreshold resolves the configured threshold or its default.
func (b *Bucket) sealThreshold() int64 {
	if b.SealThreshold > 0 {
		return b.SealThreshold
	}

	return DefaultSealThreshold
}

// clock resolves the configured clock or its default.
func (b *Bucket) clock() func() time.Time {
	if b.Clock != nil {
		return b.Clock
	}

	return time.Now
}

// logger resolves the configured logger or its default.
func (b *Bucket) logger() *slog.Logger {
	if b.Logger != nil {
		return b.Logger
	}

	return slog.Default()
}

// check reports whether the bucket is usable. Returning an error rather than
// letting a nil store panic keeps the common mistake legible.
func (b *Bucket) check() error {
	if b.Store == nil {
		return ErrNoStore
	}

	return nil
}
