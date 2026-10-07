// Package db provides a [store.Store] over one table of a PostgreSQL or
// CockroachDB database.
//
// It exists for deployments that already run such a database and no object
// store. Every object is one row: its key, its bytes, and a version counter
// that conditional writes compare. Conditions are evaluated by the database,
// so they hold across processes.
//
// It is a simple default: objects are stored whole in a BYTEA column and are
// read back whole, which suits small objects such as manifests and short
// groups.
package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strconv"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/okdaichi/qumo-ledger/ledger/store"
)

// DefaultTable is the table a [Store] uses when none is named.
const DefaultTable = "ledger_objects"

// driver is the database/sql driver the package opens databases with.
const driver = "pgx"

// tableName limits a table to an unquoted identifier, since it is spliced into
// statements.
var tableName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Store keeps objects in one table of a database.
type Store struct {
	db    *sql.DB
	table string
}

var (
	_ store.Store  = (*Store)(nil)
	_ store.Lister = (*Store)(nil)
)

// New returns a Store over table in db, creating the table if it does not
// exist. An empty table means [DefaultTable]. The caller keeps ownership of db.
func New(ctx context.Context, db *sql.DB, table string) (*Store, error) {
	if db == nil {
		return nil, errors.New("db: nil database")
	}
	if table == "" {
		table = DefaultTable
	}
	if !tableName.MatchString(table) {
		return nil, fmt.Errorf("db: invalid table name %q", table)
	}
	s := &Store{db: db, table: table}
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (
		key     TEXT   PRIMARY KEY,
		data    BYTEA  NOT NULL,
		version BIGINT NOT NULL
	)`)
	if err != nil {
		return nil, fmt.Errorf("db: create table %s: %w", table, err)
	}
	return s, nil
}

// Get implements [store.Store].
func (s *Store) Get(ctx context.Context, key string) ([]byte, store.Version, error) {
	var (
		data    []byte
		version int64
	)
	err := s.db.QueryRowContext(ctx, `SELECT data, version FROM `+s.table+` WHERE key = $1`, key).Scan(&data, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, store.NoVersion, fmt.Errorf("%w: %s", store.ErrNotExist, key)
	}
	if err != nil {
		return nil, store.NoVersion, fmt.Errorf("db: get %s: %w", key, err)
	}
	return data, formatVersion(version), nil
}

// Create implements [store.Store].
func (s *Store) Create(ctx context.Context, key string, data []byte) (store.Version, error) {
	created, err := s.insert(ctx, key, data)
	if err != nil {
		return store.NoVersion, fmt.Errorf("db: create %s: %w", key, err)
	}
	if !created {
		return store.NoVersion, fmt.Errorf("%w: %s", store.ErrExist, key)
	}
	return formatVersion(firstVersion), nil
}

// Swap implements [store.Store].
func (s *Store) Swap(ctx context.Context, key string, data []byte, expect store.Version) (store.Version, error) {
	if expect == store.NoVersion {
		created, err := s.insert(ctx, key, data)
		if err != nil {
			return store.NoVersion, fmt.Errorf("db: swap %s: %w", key, err)
		}
		if !created {
			return store.NoVersion, fmt.Errorf("%w: %s", store.ErrVersionMismatch, key)
		}
		return formatVersion(firstVersion), nil
	}

	// A version this backend never issued matches no row.
	if want, err := strconv.ParseInt(string(expect), 10, 64); err == nil {
		var next int64
		err := s.db.QueryRowContext(ctx,
			`UPDATE `+s.table+` SET data = $2, version = version + 1 WHERE key = $1 AND version = $3 RETURNING version`,
			key, notNil(data), want).Scan(&next)
		if err == nil {
			return formatVersion(next), nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return store.NoVersion, fmt.Errorf("db: swap %s: %w", key, err)
		}
	}

	// Nothing was replaced: say whether the object is absent or was changed.
	var present bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+s.table+` WHERE key = $1)`, key).Scan(&present)
	if err != nil {
		return store.NoVersion, fmt.Errorf("db: swap %s: %w", key, err)
	}
	if !present {
		return store.NoVersion, fmt.Errorf("%w: %s", store.ErrNotExist, key)
	}
	return store.NoVersion, fmt.Errorf("%w: %s", store.ErrVersionMismatch, key)
}

// Delete implements [store.Store].
func (s *Store) Delete(ctx context.Context, key string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE key = $1`, key); err != nil {
		return fmt.Errorf("db: delete %s: %w", key, err)
	}
	return nil
}

// List implements [store.Lister].
func (s *Store) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		rows, err := s.db.QueryContext(ctx,
			`SELECT key FROM `+s.table+` WHERE left(key, length($1::TEXT)) = $1::TEXT ORDER BY key`, prefix)
		if err != nil {
			yield("", fmt.Errorf("db: list %s: %w", prefix, err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				yield("", fmt.Errorf("db: list %s: %w", prefix, err))
				return
			}
			if !yield(key, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield("", fmt.Errorf("db: list %s: %w", prefix, err))
		}
	}
}

// firstVersion is the version of a newly created object.
const firstVersion = 1

// insert stores a new object and reports whether the key was free.
func (s *Store) insert(ctx context.Context, key string, data []byte) (bool, error) {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO `+s.table+` (key, data, version) VALUES ($1, $2, $3) ON CONFLICT (key) DO NOTHING`,
		key, notNil(data), firstVersion)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func formatVersion(v int64) store.Version {
	return store.Version(strconv.FormatInt(v, 10))
}

// notNil returns data, or an empty slice for nil: the column holds no NULL.
func notNil(data []byte) []byte {
	if data == nil {
		return []byte{}
	}
	return data
}
