package ledger

import (
	"log/slog"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Bucket holds tracks in an object store and is the entry point to everything
// else: a [Writer] or [Reader] is obtained from it rather than constructed
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
// Bucket is safe for concurrent use.
type Bucket struct {
	objects store.Store

	sealThreshold int64
	now           func() time.Time
	logger        *slog.Logger
}

// Option configures a [Bucket] and, through it, every Writer it opens.
type Option func(*Bucket)

// WithSealThreshold sets the open-manifest byte size that triggers a seal.
func WithSealThreshold(bytes int64) Option {
	return func(b *Bucket) {
		if bytes > 0 {
			b.sealThreshold = bytes
		}
	}
}

// WithClock replaces the wallclock source, for tests and for producers that
// supply their own notion of ingest time.
func WithClock(now func() time.Time) Option {
	return func(b *Bucket) {
		if now != nil {
			b.now = now
		}
	}
}

// WithLogger sets the logger used for events that do not affect correctness,
// such as a failed head update.
func WithLogger(logger *slog.Logger) Option {
	return func(b *Bucket) {
		if logger != nil {
			b.logger = logger
		}
	}
}

// New binds an object store as a bucket of tracks.
//
// It performs no I/O — nothing is read or written until a track is created,
// opened, or appended to — so it cannot fail and returns no error.
func New(objects store.Store, opts ...Option) *Bucket {
	b := &Bucket{
		objects:       objects,
		sealThreshold: DefaultSealThreshold,
		now:           time.Now,
		logger:        slog.Default(),
	}
	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Store returns the object store the bucket was built on, so that tooling —
// garbage collection, a backup pass — can reach it without being handed the
// store separately.
func (b *Bucket) Store() store.Store { return b.objects }
