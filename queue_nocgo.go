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

    if err != nil {
        return nil, errors.Trace(err)
    }

    return db, nil
}
