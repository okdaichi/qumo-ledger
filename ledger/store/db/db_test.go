package db_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/db"
	"github.com/okdaichi/qumo-ledger/ledger/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uriEnv names the PostgreSQL or CockroachDB database the tests run against.
// They are skipped when it is unset.
const uriEnv = "SQLSTORE_TEST_URI"

var tableCount atomic.Int64

// newStore returns a Store over a table of its own, dropped when the test ends.
func newStore(t *testing.T) *db.Store {
	t.Helper()
	uri := os.Getenv(uriEnv)
	if uri == "" {
		t.Skipf("%s is not set", uriEnv)
	}
	conn, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	table := fmt.Sprintf("ledger_test_%d_%d", time.Now().UnixNano(), tableCount.Add(1))
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table)
		_ = conn.Close()
	})

	s, err := db.New(t.Context(), conn, table)
	require.NoError(t, err)
	return s
}

func TestStore_Conformance(t *testing.T) {
	storetest.Run(t, newStore)
}

func TestStore_ListerConformance(t *testing.T) {
	storetest.RunLister(t, newStore)
}

func TestStore_SwapWithForeignVersion(t *testing.T) {
	s := newStore(t)
	_, err := s.Create(t.Context(), "head", []byte("a"))
	require.NoError(t, err)

	_, err = s.Swap(t.Context(), "head", []byte("b"), store.Version(`"an-etag"`))

	assert.ErrorIs(t, err, store.ErrVersionMismatch)
}

func TestOpen_UsesTheNamedTable(t *testing.T) {
	uri := os.Getenv(uriEnv)
	if uri == "" {
		t.Skipf("%s is not set", uriEnv)
	}
	table := fmt.Sprintf("ledger_test_open_%d", time.Now().UnixNano())
	conn, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table)
		_ = conn.Close()
	})

	opened, err := store.Open(t.Context(), uri+"&table="+table)
	require.NoError(t, err)
	_, err = opened.Create(t.Context(), "key", []byte("value"))
	require.NoError(t, err)

	var n int
	require.NoError(t, conn.QueryRowContext(t.Context(), `SELECT count(*) FROM `+table).Scan(&n))
	assert.Equal(t, 1, n)
}

func TestStore_DatabaseErrors(t *testing.T) {
	uri := os.Getenv(uriEnv)
	if uri == "" {
		t.Skipf("%s is not set", uriEnv)
	}
	conn, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	s, err := db.New(t.Context(), conn, "")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	calls := map[string]func(ctx context.Context) error{
		"get":                func(ctx context.Context) error { _, _, err := s.Get(ctx, "k"); return err },
		"create":             func(ctx context.Context) error { _, err := s.Create(ctx, "k", nil); return err },
		"swap to create":     func(ctx context.Context) error { _, err := s.Swap(ctx, "k", nil, store.NoVersion); return err },
		"swap a version":     func(ctx context.Context) error { _, err := s.Swap(ctx, "k", nil, "1"); return err },
		"swap a foreign one": func(ctx context.Context) error { _, err := s.Swap(ctx, "k", nil, `"etag"`); return err },
		"delete":             func(ctx context.Context) error { return s.Delete(ctx, "k") },
		"list": func(ctx context.Context) error {
			for _, err := range s.List(ctx, "") {
				return err
			}
			return nil
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call(t.Context())

			require.Error(t, err)
			assert.ErrorContains(t, err, "db:")
			assert.NotErrorIs(t, err, store.ErrNotExist)
			assert.NotErrorIs(t, err, store.ErrVersionMismatch)
		})
	}
}

func TestStore_List_StopsWhenTheCallerDoes(t *testing.T) {
	s := newStore(t)
	for i := range 3 {
		_, err := s.Create(t.Context(), fmt.Sprintf("k%d", i), nil)
		require.NoError(t, err)
	}

	var seen []string
	for key, err := range s.List(t.Context(), "") {
		require.NoError(t, err)
		seen = append(seen, key)
		break
	}

	assert.Equal(t, []string{"k0"}, seen)
}
