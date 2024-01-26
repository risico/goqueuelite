//go:build !nocgo

package goqueuelite

import (
	"database/sql"

	"github.com/juju/errors"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Registers the sqlite3 driver with a ConnectHook so that we can
	// initialize the default PRAGMAs.
	//
	// Note 1: we don't define the PRAGMA as part of the dsn string
	// because not all pragmas are available.
	//
	// Note 2: the busy_timeout pragma must be first because
	// the connection needs to be set to block on busy before WAL mode
	// is set in case it hasn't been already set by another connection.
	sql.Register("cgo_sqlite3",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				return nil
			},
		},
	)
}

func openDB(dsn string) (*sql.DB, error) {
    db, err := sql.Open("cgo_sqlite3", dsn)
    if err != nil {
        return nil, errors.Trace(err)
    }

    return db, nil
}

