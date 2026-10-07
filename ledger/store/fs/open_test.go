package fs

import (
	"net/url"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURIPath(t *testing.T) {
	tests := map[string]struct {
		uri     string
		want    string
		wantErr bool
	}{
		"absolute":       {uri: "file:///var/lib/ledger", want: filepath.FromSlash("/var/lib/ledger")},
		"relative":       {uri: "file:ledger/data", want: filepath.FromSlash("ledger/data")},
		"windows drive":  {uri: "file:///C:/ledger", want: filepath.FromSlash("C:/ledger")},
		"localhost":      {uri: "file://localhost/var/lib/ledger", want: filepath.FromSlash("/var/lib/ledger")},
		"another host":   {uri: "file://example.com/var/lib/ledger", wantErr: true},
		"no path":        {uri: "file://", wantErr: true},
		"escaped spaces": {uri: "file:///srv/my%20ledger", want: filepath.FromSlash("/srv/my ledger")},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(tt.uri)
			require.NoError(t, err)

			got, err := uriPath(u)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestOpen_RegistersTheFileScheme(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ledger")
	uri := (&url.URL{Scheme: Scheme, Path: "/" + filepath.ToSlash(dir)}).String()

	s, err := store.Open(t.Context(), uri)

	require.NoError(t, err)
	require.IsType(t, &Store{}, s)
	_, err = s.Create(t.Context(), "key", []byte("value"))
	require.NoError(t, err)
	assert.FileExists(t, filepath.Join(dir, "key"))
}

func TestOpen_FileURIErrorHidesThePassword(t *testing.T) {
	_, err := store.Open(t.Context(), "file://user:hunter2@example.com/ledger")

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hunter2")
}
