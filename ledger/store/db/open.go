package db

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// Scheme and SchemeAlias are the URI schemes [store.Open] opens with this
// backend. The URI is an ordinary PostgreSQL connection URI, which CockroachDB
// accepts too:
//
//	postgres://user:password@host:5432/database?sslmode=verify-full
//
// The query parameter "table" names the table and is not passed to the
// database; without it the table is [DefaultTable].
const (
	Scheme      = "postgres"
	SchemeAlias = "postgresql"
)

func init() {
	for _, scheme := range []string{Scheme, SchemeAlias} {
		store.Register(scheme, func(ctx context.Context, u *url.URL) (store.Store, error) {
			return Open(ctx, u.String())
		})
	}
}

// Open connects to the database a PostgreSQL connection URI names and returns
// a Store over its table. The connection pool lives as long as the process.
func Open(ctx context.Context, uri string) (*Store, error) {
	dsn, table, err := splitTable(uri)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("db: open database: %w", err)
	}
	s, err := New(ctx, db, table)
	if err != nil {
		_ = db.Close() // not actionable: the error from New is the one to report
		return nil, err
	}
	return s, nil
}

// splitTable removes the "table" query parameter from a connection URI and
// returns the rest with the table it named.
func splitTable(uri string) (dsn, table string, err error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("db: parse connection URI: %w", err)
	}
	query := u.Query()
	table = query.Get("table")
	query.Del("table")
	u.RawQuery = query.Encode()
	return u.String(), table, nil
}
