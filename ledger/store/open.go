package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
)

// MemoryScheme is the scheme [Open] uses for an empty URI.
const MemoryScheme = "mem"

// ErrUnknownScheme reports a URI whose scheme no registered backend opens.
var ErrUnknownScheme = errors.New("store: unknown scheme")

// Opener opens the store a URI names. u is never nil and its scheme is the one
// the opener was registered under.
type Opener func(ctx context.Context, u *url.URL) (Store, error)

var (
	openersMu sync.RWMutex
	openers   = make(map[string]Opener)
)

// Register makes a backend available to [Open] under a URI scheme. A backend
// calls it from an init function, so importing the backend's package is what
// enables its scheme:
//
//	import _ "github.com/okdaichi/qumo-ledger/ledger/store/fs"
//
// Register panics if open is nil or the scheme is already registered.
func Register(scheme string, open Opener) {
	openersMu.Lock()
	defer openersMu.Unlock()
	if open == nil {
		panic("store: Register opener is nil")
	}
	if _, dup := openers[scheme]; dup {
		panic("store: Register called twice for scheme " + scheme)
	}
	openers[scheme] = open
}

// Schemes returns the registered schemes, sorted.
func Schemes() []string {
	openersMu.RLock()
	defer openersMu.RUnlock()
	schemes := make([]string, 0, len(openers))
	for scheme := range openers {
		schemes = append(schemes, scheme)
	}
	slices.Sort(schemes)
	return schemes
}

// Open opens the store a URI names. The scheme selects the backend, which must
// be registered:
//
//	""                                  memory, the same as "mem:"
//	mem:                                memory                 (mem)
//	file:///var/lib/ledger              a directory            (fs)
//	s3://bucket/prefix                  an S3-compatible store (bucket)
//	postgres://user@host:26257/db       a SQL table            (db)
//
// A URI with no scheme other than the empty string is an error, so a bare path
// is never mistaken for a backend. Open returns [ErrUnknownScheme] when no
// registered backend opens the scheme.
func Open(ctx context.Context, uri string) (Store, error) {
	if uri == "" {
		uri = MemoryScheme + ":"
	}
	// A URI can carry a password, so errors name it only redacted, and a
	// parse error, which quotes the whole URI, not at all.
	u, err := url.Parse(uri)
	if err != nil {
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return nil, fmt.Errorf("store: parse URI: %w", err)
	}
	openersMu.RLock()
	open, ok := openers[u.Scheme]
	openersMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w %q in %q (registered: %v)", ErrUnknownScheme, u.Scheme, u.Redacted(), Schemes())
	}
	return open(ctx, u)
}
