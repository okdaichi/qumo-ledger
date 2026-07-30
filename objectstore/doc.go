// Package objectstore defines the storage substrate qumo-ledger is built on.
//
// The ledger is object-store native: every durable artifact is an object, and a
// reader holding nothing but object-store access can seek and replay a track
// without any ledger process running. That invariant constrains this interface
// more than it might first appear.
//
// # Required primitives
//
// Almost everything the ledger writes is immutable, so the primitive that must
// work uniformly across every backend is conditional *create* — [Store.Create],
// which fails with [ErrExist] rather than overwriting. S3 spells it
// If-None-Match, GCS spells it ifGenerationMatch=0, and a local filesystem
// spells it O_EXCL. All three are reliable.
//
// Exactly one object per track is mutable: the head pointer. It is the only
// reason [Store.Swap] exists. Backends that cannot implement compare-and-swap
// can still be used for reading.
//
// # Deliberately optional
//
// Listing is not part of [Store]. The read path never lists: readers discover
// the tip through the head pointer and then probe forward by deterministic key.
// That is a cost decision as much as a correctness one — S3 LIST is roughly an
// order of magnitude more expensive per request than GET and carries the
// weakest consistency guarantees of any operation in the API. Only garbage
// collection needs enumeration, so it lives on the optional [Lister].
//
// [Presigner] is likewise optional. Authorization is not the ledger's job: an
// external service is expected to mint scoped credentials or signed URLs so
// that clients keep reading objects directly. Backends that can presign let
// that service delegate instead of proxying bytes.
package objectstore
