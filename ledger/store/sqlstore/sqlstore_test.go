package sqlstore_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/okdaichi/qumo-ledger/ledger/store/sqlstore"
	"github.com/okdaichi/qumo-ledger/ledger/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uriEnv names the PostgreSQL or CockroachDB database the tests run against.
// They are skipped when it is unset.
const uriEnv = "SQLSTORE_TEST_URI"

var tableCount atomic.Int64

// newStore returns a Store over a table of its own, dropped when the test ends.
func newStore(t *testing.T) *sqlstore.Store {
	t.Helper()
	uri := os.Getenv(uriEnv)
	if uri == "" {
		t.Skipf("%s is not set", uriEnv)
	}
	db, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	table := fmt.Sprintf("ledger_test_%d_%d", time.Now().UnixNano(), tableCount.Add(1))
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table)
		_ = db.Close()
	})

	s, err := sqlstore.New(t.Context(), db, table)
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

func TestNew_Rejected(t *testing.T) {
	_, err := sqlstore.New(t.Context(), nil, "")
	assert.Error(t, err, "a nil database")

	db, err := sql.Open("pgx", "postgres://localhost/unused")
	require.NoError(t, err)
	defer db.Close()

	_, err = sqlstore.New(t.Context(), db, "objects; DROP TABLE users")
	assert.Error(t, err, "a table name that is not an identifier")
}

func TestOpen_UsesTheNamedTable(t *testing.T) {
	uri := os.Getenv(uriEnv)
	if uri == "" {
		t.Skipf("%s is not set", uriEnv)
	}
	table := fmt.Sprintf("ledger_test_open_%d", time.Now().UnixNano())
	db, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+table)
		_ = db.Close()
	})

	opened, err := store.Open(t.Context(), uri+"&table="+table)
	require.NoError(t, err)
	_, err = opened.Create(t.Context(), "key", []byte("value"))
	require.NoError(t, err)

	var n int
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT count(*) FROM `+table).Scan(&n))
	assert.Equal(t, 1, n)
}
