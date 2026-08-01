package store

import (
	"context"
	"errors"
	"iter"
)

// Sentinel errors returned by every backend. Callers match with errors.Is, so
// backends must wrap rather than replace these.
var (
	// ErrNotExist reports that no object is stored under the key.
	ErrNotExist = errors.New("store: object does not exist")

	// ErrExist reports that Create found an object already stored under the
	// key. Because ledger objects are immutable, this is a normal control-flow
	// signal rather than a failure: it is how a duplicate append is rejected
	// and how a zombie writer is fenced off after a failover.
	ErrExist = errors.New("store: object already exists")

	// ErrVersionMismatch reports that Swap found a version other than the one
	// the caller expected, meaning someone else wrote first.
	ErrVersionMismatch = errors.New("store: version mismatch")
)

// Version identifies one revision of an object, mapping onto an S3 ETag, a GCS
// generation, or an Azure ETag. It is opaque: callers may compare versions for
// equality and hand them back to [Store.Swap], nothing more.
type Version string

// NoVersion is the zero Version. Passed to [Store.Swap] it means "succeed only
// if the object does not yet exist", matching If-None-Match: *.
const NoVersion Version = ""

// Store is the minimum a backend must provide for the ledger to read and write
// a track. See the package documentation for why listing is not included.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// Get returns the object stored under key along with its current version.
	// It returns ErrNotExist if the key is absent — which is an expected
	// result on the read path, where probing forward for the next delta
	// manifest treats absence as "not committed yet".
	Get(ctx context.Context, key string) ([]byte, Version, error)

	// Create stores data under key only if the key is currently absent,
	// returning ErrExist otherwise. It is the workhorse of the ledger: group
	// objects and delta manifests are written exactly once and never modified,
	// so this is the only write most backends ever see.
	Create(ctx context.Context, key string, data []byte) (Version, error)

	// Swap replaces the object under key only if its current version equals
	// expect, returning ErrVersionMismatch otherwise. Pass NoVersion to
	// require that the object be absent.
	//
	// Only the head pointer needs this. Every store the ledger targets can
	// provide it — S3 conditional writes, GCS generation preconditions, Azure
	// ETags, a local rename — so it is required rather than optional.
	Swap(ctx context.Context, key string, data []byte, expect Version) (Version, error)

	// Delete removes the object under key. Deleting an absent key is not an
	// error, so garbage collection can be retried freely.
	Delete(ctx context.Context, key string) error
}

// Lister enumerates keys. It exists solely for garbage collection, which must
// find group objects that no manifest references. The read path never lists.
type Lister interface {
	// List iterates every key under prefix in unspecified order. Iteration
	// stops at the first error yielded.
	List(ctx context.Context, prefix string) iter.Seq2[string, error]
}
