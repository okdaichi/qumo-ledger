package ledger_test

import (
	"fmt"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/fs"
	"github.com/okdaichi/qumo-ledger/ledger/store/mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenStore(t *testing.T) {
	t.Run("MemoryEmpty", func(t *testing.T) {
		s, err := ledger.OpenStore(t.Context(), "")
		require.NoError(t, err)
		assert.IsType(t, &mem.Store{}, s)
		assert.Equal(t, "memory (lost on exit)", fmt.Sprint(s))
	})

	t.Run("File", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "ledger")
		uri := (&url.URL{Scheme: "file", Path: "/" + filepath.ToSlash(dir)}).String()

		s, err := ledger.OpenStore(t.Context(), uri)
		require.NoError(t, err)
		assert.IsType(t, &fs.Store{}, s)
		assert.Contains(t, fmt.Sprint(s), "file://")
	})

	t.Run("RelativeFile", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Chdir(tempDir)

		s, err := ledger.OpenStore(t.Context(), "file:ledger")
		require.NoError(t, err)
		assert.IsType(t, &fs.Store{}, s)
		assert.Equal(t, "file:ledger", fmt.Sprint(s))
	})

	t.Run("RegistersEveryBackend", func(t *testing.T) {
		_, err := ledger.OpenStore(t.Context(), "")
		require.NoError(t, err)
		assert.Subset(t, store.Schemes(), []string{"file", "mem", "postgres", "postgresql", "s3"})
	})
}
