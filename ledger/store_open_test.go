package ledger_test

import (
	"fmt"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger"
	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/bucket"
	"github.com/okdaichi/qumo-ledger/ledger/store/db"
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

	t.Run("BucketStringer", func(t *testing.T) {
		s, err := bucket.New(bucket.Config{
			Bucket:          "my-bucket",
			Prefix:          "tenant/a",
			Region:          "us-east-1",
			AccessKeyID:     "id",
			SecretAccessKey: "secret",
		})
		require.NoError(t, err)
		assert.Equal(t, "s3://my-bucket/tenant/a", fmt.Sprint(s))
	})

	t.Run("DBStringerFallback", func(t *testing.T) {
		var s db.Store
		assert.Equal(t, "postgres:ledger_objects", fmt.Sprint(&s))
	})

	t.Run("NilStringer", func(t *testing.T) {
		var memStore *mem.Store
		var fsStore *fs.Store
		var bucketStore *bucket.Store
		var dbStore *db.Store

		assert.Equal(t, "<nil>", fmt.Sprint(memStore))
		assert.Equal(t, "<nil>", fmt.Sprint(fsStore))
		assert.Equal(t, "<nil>", fmt.Sprint(bucketStore))
		assert.Equal(t, "<nil>", fmt.Sprint(dbStore))
	})

	t.Run("RegistersEveryBackend", func(t *testing.T) {
		_, err := ledger.OpenStore(t.Context(), "")
		require.NoError(t, err)
		assert.Subset(t, store.Schemes(), []string{"file", "mem", "postgres", "postgresql", "s3"})
	})

	t.Run("UnknownScheme", func(t *testing.T) {
		_, err := ledger.OpenStore(t.Context(), "unknown://host/dir")
		require.Error(t, err)
		assert.ErrorIs(t, err, store.ErrUnknownScheme)
	})

	t.Run("InvalidURI", func(t *testing.T) {
		_, err := ledger.OpenStore(t.Context(), "://invalid")
		require.Error(t, err)
	})
}
