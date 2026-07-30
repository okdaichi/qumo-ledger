// Package fsstore provides a local-filesystem [objectstore.Store].
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

	"github.com/okdaichi/qumo-ledger/objectstore"
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
	_ objectstore.Store  = (*Store)(nil)
	_ objectstore.Lister = (*Store)(nil)
)

// New returns a Store rooted at dir, creating dir if it does not exist.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("fsstore: create root %q: %w", dir, err)
	}

	return &Store{root: dir}, nil
}

// Get implements [objectstore.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return nil, objectstore.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return nil, objectstore.NoVersion, err
	}

	data, err := os.ReadFile(name)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, objectstore.NoVersion, fmt.Errorf("fsstore: get %q: %w", key, objectstore.ErrNotExist)
		}
		return nil, objectstore.NoVersion, fmt.Errorf("fsstore: get %q: %w", key, err)
	}

	return data, version(data), nil
}

// Create implements [objectstore.Store]. Exclusivity comes from O_EXCL, so it
// is atomic against other processes as well as other goroutines.
func (s *Store) Create(ctx context.Context, key string, data []byte) (objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return objectstore.NoVersion, err
	}

	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, err)
	}

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return objectstore.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, objectstore.ErrExist)
		}
		return objectstore.NoVersion, fmt.Errorf("fsstore: create %q: %w", key, err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: write %q: %w", key, err)
	}

	// Objects are immutable and referenced only after the write returns, so a
	// torn file after a crash is indistinguishable from one that was never
	// created — both are simply absent from any manifest.
	if err := f.Sync(); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: sync %q: %w", key, err)
	}

	return version(data), nil
}

// Swap implements [objectstore.Store] via a temporary file and rename, which
// is atomic on both POSIX and Windows. See the package docs for the
// single-process caveat.
func (s *Store) Swap(ctx context.Context, key string, data []byte, expect objectstore.Version) (objectstore.Version, error) {
	if err := ctx.Err(); err != nil {
		return objectstore.NoVersion, err
	}

	name, err := s.resolve(key)
	if err != nil {
		return objectstore.NoVersion, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := os.ReadFile(name)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if expect != objectstore.NoVersion {
			return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, objectstore.ErrNotExist)
		}
	case err != nil:
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	default:
		if version(current) != expect {
			return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, objectstore.ErrVersionMismatch)
		}
	}

	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(name), ".swap-*")
	if err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}
	if err := tmp.Close(); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	if err := os.Rename(tmp.Name(), name); err != nil {
		return objectstore.NoVersion, fmt.Errorf("fsstore: swap %q: %w", key, err)
	}

	return version(data), nil
}

// Delete implements [objectstore.Store].
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

// Keys implements [objectstore.Lister].
func (s *Store) Keys(ctx context.Context, prefix string) iter.Seq2[string, error] {
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
// would escape the root.
func (s *Store) resolve(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("%w: empty", ErrInvalidKey)
	}
	if path.IsAbs(key) || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("%w: %q is absolute", ErrInvalidKey, key)
	}

	clean := path.Clean(key)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: %q escapes the root", ErrInvalidKey, key)
	}

	return filepath.Join(s.root, filepath.FromSlash(clean)), nil
}

// version derives an object version from its content. A real backend would
// return the ETag it already maintains; deriving it here keeps the semantics
// identical without a sidecar file or extended attributes.
func version(data []byte) objectstore.Version {
	h := fnv.New64a()
	_, _ = h.Write(data)

	return objectstore.Version(strconv.FormatUint(h.Sum64(), 16))
}
