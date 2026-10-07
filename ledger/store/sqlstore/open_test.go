package sqlstore

import (
	"database/sql"
	"testing"

	"github.com/okdaichi/qumo-ledger/ledger/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitTable(t *testing.T) {
	tests := map[string]struct {
		uri       string
		wantDSN   string
		wantTable string
		wantErr   bool
	}{
		"no table":             {uri: "postgres://u@db:26257/ledger?sslmode=disable", wantDSN: "postgres://u@db:26257/ledger?sslmode=disable"},
		"a table":              {uri: "postgres://u@db/ledger?sslmode=disable&table=chat", wantDSN: "postgres://u@db/ledger?sslmode=disable", wantTable: "chat"},
		"only a table":         {uri: "postgresql://u@db/ledger?table=chat", wantDSN: "postgresql://u@db/ledger", wantTable: "chat"},
		"the password is kept": {uri: "postgres://u:p%40ss@db/ledger?table=chat", wantDSN: "postgres://u:p%40ss@db/ledger", wantTable: "chat"},
		"unparsable":           {uri: "postgres://u@db:port/ledger", wantErr: true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			dsn, table, err := splitTable(tt.uri)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDSN, dsn)
			assert.Equal(t, tt.wantTable, table)
		})
	}
}

func TestNew_Rejected(t *testing.T) {
	db, err := sql.Open(driver, "postgres://localhost/unused")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() }) // not actionable: nothing was connected

	tests := map[string]struct {
		db    *sql.DB
		table string
	}{
		"a nil database":                 {table: DefaultTable},
		"a table name with a statement":  {db: db, table: "objects; DROP TABLE users"},
		"a table name with a dot":        {db: db, table: "public.objects"},
		"a table name starting a number": {db: db, table: "1objects"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := New(t.Context(), tt.db, tt.table)

			assert.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestOpen_RefusesWhatItCannotUse(t *testing.T) {
	tests := map[string]string{
		"an unparsable URI":  "postgres://u@db:port/ledger",
		"a bad table name":   "postgres://u@127.0.0.1:1/ledger?table=a-b",
		"no database server": "postgres://u@127.0.0.1:1/ledger?sslmode=disable&connect_timeout=1",
	}
	for name, uri := range tests {
		t.Run(name, func(t *testing.T) {
			s, err := store.Open(t.Context(), uri)

			assert.Error(t, err)
			assert.Nil(t, s)
		})
	}
}

func TestVersion(t *testing.T) {
	assert.Equal(t, store.Version("1"), formatVersion(firstVersion))
	assert.Equal(t, []byte{}, notNil(nil), "the column holds no NULL")
	assert.Equal(t, []byte("x"), notNil([]byte("x")))
}
