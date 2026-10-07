package fsstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Scheme is the URI scheme [store.Open] opens with this backend:
//
//	file:///var/lib/ledger    the directory /var/lib/ledger
//	file:ledger               the directory ledger, relative to the working directory
//	file:///C:/ledger         a Windows path with a drive
const Scheme = "file"

func init() {
	store.Register(Scheme, func(_ context.Context, u *url.URL) (store.Store, error) {
		dir, err := uriPath(u)
		if err != nil {
			return nil, fmt.Errorf("fsstore: %q: %w", u.String(), err)
		}
		return New(dir)
	})
}

// uriPath returns the directory a file URI names.
func uriPath(u *url.URL) (string, error) {
	if u.Host != "" && u.Host != "localhost" {
		return "", fmt.Errorf("a file URI names no host, got %q", u.Host)
	}
	// file:relative/dir carries its path as Opaque; file:///abs/dir as Path.
	dir := u.Opaque
	if dir == "" {
		dir = u.Path
	}
	if dir == "" {
		return "", errors.New("a file URI needs a path")
	}
	// file:///C:/dir parses to the path /C:/dir.
	if len(dir) >= 3 && dir[0] == '/' && dir[2] == ':' {
		dir = dir[1:]
	}
	return filepath.FromSlash(dir), nil
}
