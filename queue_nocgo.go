//go:build nocgo

package goqueuelite

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	_ "modernc.org/sqlite"
)

func openDB(dsn string) (*sql.DB, error) {
    db, err := sql.Open("sqlite3", dsn)
    if err != nil {
        return nil, errors.Trace(err)
    }

    _, err = db.Exec(`
        PRAGMA busy_timeout       = 10000;
        PRAGMA journal_mode       = WAL;
        PRAGMA journal_size_limit = 200000000;
        PRAGMA synchronous        = NORMAL;
        PRAGMA foreign_keys       = ON;
        PRAGMA temp_store         = MEMORY;
        PRAGMA cache_size         = -16000;
    `)

    if err != nil {
        return nil, errors.Trace(err)
    }

    fmt.Println("sqlite3 driver is used")

    return db, nil
}
