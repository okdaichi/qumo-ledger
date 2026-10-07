package store_test

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/fsstore"
	"github.com/okdaichi/qumo-ledger/ledger/store/memstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpen_Memory(t *testing.T) {
	for name, uri := range map[string]string{"empty": "", "scheme": "mem:"} {
		t.Run(name, func(t *testing.T) {
			s, err := store.Open(t.Context(), uri)

			require.NoError(t, err)
			assert.IsType(t, &memstore.Store{}, s)
		})
	}
}

func TestOpen_File(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ledger")
	uri := (&url.URL{Scheme: "file", Path: "/" + filepath.ToSlash(dir)}).String()

	s, err := store.Open(t.Context(), uri)

	require.NoError(t, err)
	assert.IsType(t, &fsstore.Store{}, s)
	assert.DirExists(t, dir)
}

func TestOpen_Rejected(t *testing.T) {
	tests := map[string]struct {
		uri         string
		wantUnknown bool
	}{
		"a bare path":         {uri: "./ledger", wantUnknown: true},
		"an unknown scheme":   {uri: "gopher://host/ledger", wantUnknown: true},
		"a file URI on host":  {uri: "file://example.com/var/lib/ledger"},
		"a file URI, no path": {uri: "file://"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := store.Open(t.Context(), tt.uri)

			require.Error(t, err)
			assert.Nil(t, s)
			assert.Equal(t, tt.wantUnknown, errors.Is(err, store.ErrUnknownScheme))
		})
	}
}

func TestSchemes_ListsRegisteredBackends(t *testing.T) {
	schemes := store.Schemes()

	assert.Contains(t, schemes, memstore.Scheme)
	assert.Contains(t, schemes, fsstore.Scheme)
	assert.IsIncreasing(t, schemes)
}

func TestRegister_Panics(t *testing.T) {
	opener := func(context.Context, *url.URL) (store.Store, error) { return memstore.New(), nil }

	assert.Panics(t, func() { store.Register("nil-opener", nil) })
	assert.Panics(t, func() { store.Register(memstore.Scheme, opener) }, "a scheme is registered once")
}
