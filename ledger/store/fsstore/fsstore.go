// Package fsstore provides a local-filesystem [store.Store].
//
// It exists so the ledger can run with no cloud dependency — for development,
// for single-node deployments, and for tests that want real durability. Object
// keys map to paths under a root directory, so the on-disk tree is browsable
// and a track can be inspected with ordinary shell tools.
//
// # Limits
//
// Compare-and-swap is guarded by an in-process mutex, so it is atomic only
// within a single process. Two processes writing the same track through
// separate Store values can race. That is acceptable because a track has
// exactly one writer by design, but it does mean this backend cannot fence a
// zombie writer the way S3 conditional writes can.
package fsstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// ErrInvalidKey reports a key that does not name a location inside the root.
var ErrInvalidKey = errors.New("fsstore: invalid key")

// Store maps object keys onto files under a root directory.
type Store struct {
	root string

	// mu serializes Swap's read-compare-write. Create needs no lock because
	// O_EXCL is atomic in the kernel.
	mu sync.Mutex
}

var (
	_ store.Store  = (*Store)(nil)
	_ store.Lister = (*Store)(nil)
)

// New returns a Store rooted at dir, creating dir if it does not exist.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("fsstore: create root %q: %w", dir, err)
	}

	return &Store{root: dir}, nil
}

// Get implements [store.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	if err := ctx.Err(); err != nil {
		return nil, store.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return nil, store.NoVersion, err
	}

	data, err := os.ReadFile(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, store.NoVersion, fmt.Errorf("fsstore: get %q: %w", key, store.ErrNotExist)
		}
		return nil, store.NoVersion, fmt.Errorf("fsstore: get %q: %w", key, err)
	}

	return data, version(data), nil
}

// Create implements [store.Store]. Exclusivity comes from O_EXCL, so it
// is atomic against other processes as well as other goroutines.
func (s *Store) Create(ctx context.Context, key string, data []byte) (store.Version, error) {
	if err := ctx.Err(); err != nil {
		return store.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return store.NoVersion, err
	}

	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, err)
	}

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return store.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, store.ErrExist)
		}
		return store.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: write %q: %w", key, err)
	}

	// Objects are immutable and referenced only after the write returns, so a
	// torn file after a crash is indistinguishable from one that was never
	// created — both are simply absent from any manifest.
	if err := f.Sync(); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: sync %q: %w", key, err)
	}

	return version(data), nil
}

// Swap implements [store.Store] via a temporary file and rename, which
// is atomic on both POSIX and Windows. See the package docs for the
// single-process caveat.
func (s *Store) Swap(ctx context.Context, key string, data []byte, expect store.Version) (store.Version, error) {
	if err := ctx.Err(); err != nil {
		return store.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return store.NoVersion, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := os.ReadFile(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if expect != store.NoVersion {
			return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, store.ErrNotExist)
		}
	case err != nil:
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	default:
		if version(current) != expect {
			return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, store.ErrVersionMismatch)
		}
	}

	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(name), ".swap-*")
	if err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	if err := os.Rename(tmp.Name(), name); err != nil {
		return store.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	return version(data), nil
}

// Delete implements [store.Store].
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	name, err := s.resolve(key)
	if err != nil {
		return err
	}

	if err := os.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("fsstore: delete %q: %w", key, err)
	}

	return nil
}

// List implements [store.Lister].
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		walkErr := filepath.WalkDir(s.root, func(name string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			rel, err := filepath.Rel(s.root, name)
			if err != nil {
				return err
			}

			key := filepath.ToSlash(rel)
			if !strings.HasPrefix(key, prefix) {
				return nil
			}
			if !yield(key, nil) {
				return fs.SkipAll
			}

			return nil
		})

		if walkErr != nil && !errors.Is(walkErr, fs.SkipAll) {
			yield("", fmt.Errorf("fsstore: list %q: %w", prefix, walkErr))
		}
	}
}

// resolve maps an object key onto a filesystem path, rejecting anything that
// would escape the root or name two objects with one key.
//
// Keys are not all self-authored: a reader takes a group's key from
// GroupInfo.ObjectKey, which is manifest data, so resolve is a trust boundary
// rather than a formatting helper.
func (s *Store) resolve(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidKey)
	}

	// Backslashes are rejected on every platform, not just Windows. A key is
	// slash-separated by definition, so a backslash would otherwise be an
	// ordinary character on Linux and a separator on Windows — the same key
	// naming different objects depending on the host.
	if strings.ContainsRune(key, '\\') {
		return "", fmt.Errorf("%w: %q contains a backslash", ErrInvalidKey, key)
	}

	// Keys must be canonical. Without this, "a/./b" and "a/b" resolve to one
	// file, so a Create under the second would report ErrExist for an object
	// the caller never wrote — and immutability depends on one key naming
	// exactly one object.
	if path.Clean(key) != key {
		return "", fmt.Errorf("%w: %q is not clean", ErrInvalidKey, key)
	}

	// IsLocal rejects absolute paths and parent traversal, and on Windows also
	// rejects reserved device names such as NUL and COM1 — which are otherwise
	// legal-looking relative keys that open a device instead of a file.
	local := filepath.FromSlash(key)
	if !filepath.IsLocal(local) {
		return "", fmt.Errorf("%w: %q is not local to the root", ErrInvalidKey, key)
	}

	return filepath.Join(s.root, local), nil
}

// version derives an object version from its content. A real backend would
// return the ETag it already maintains; deriving it here keeps the semantics
// identical without a sidecar file or extended attributes.
func version(data []byte) store.Version {
	h := fnv.New64a()
	// not actionable: hash.Hash.Write is documented never to return an error.
	_, _ = h.Write(data)

	return store.Version(strconv.FormatUint(h.Sum64(), 16))
}
