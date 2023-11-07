package squeuelite

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/risico/clock"
)

type JobStatus int

const (
	JobStatusReady JobStatus = iota
	JobStatusLocked
	JobStatusDone
	JobStatusFailed
)

// preparedStatement is a type to help us keep track of our prepared statements
type preparedStatement int

const (
	enqueueStatement preparedStatement = iota
	dequeueStatement
	updateStatusStatement
	updateStatusRetryStatement
)

type Message struct {
	// Data holds the data for this job
	ID          int64
	Data        any
	Namespace   string
	Status      JobStatus
	Delay       uint64
	LockTime    int
	DoneTime    int
	Retries     int
	ScheduledAt int
	TTL         int
}

type Queue interface {
	// EnqueueWithParams adds a new job to the Queue with custom parameters
	Enqueue(data any, params EnqueueParams) (int64, error)
	// Dequeue returns the next job in the Queue
	Dequeue(params DequeueParams) (*Message, error)
	// Done marks the job as done
	Done(id int64) error
	// Fail marks the job as failed
	Fail(id int64) error
	// Retry marks the message as ready to be consumed again
	Retry(id int64) error
	// Size returns the size of the queue
	Size() (int, error)
	// Prune deletes completed jobs
	Prune() error

	Close() error
}

var _ Queue = new(SqliteQueue)

// Params are passed into the Queue and accept external user input
type Params struct {
	// DB is the main link to the database, you can either pass this from outside
	// or if left nil it will try to create it
	DB *sql.DB

	Clock clock.Clock

	// DatabasePath is the path where the database sits (if no sql.DB is being passed)
	DatabasePath string

	// AutoVacuum automatically handles vaccuming the db, if this is not
	// enabled you will have to take care of it by manually calling Queue.Vacuum
	AutoVacuum         bool
	AutoVacuumInterval time.Duration

	// AutoPrune deletes completed jobs
	AutoPrune         bool
	AutoPruneInterval time.Duration

	// DefaultTTL is the default time to live for a job
	DefaultTTL time.Duration
}

func (q Params) Defaults() (Params, error) {
	if q.DatabasePath == "" {
		q.DatabasePath = "file:queue.db"
	}

	if q.DatabasePath != ":memory:" && !strings.HasPrefix(q.DatabasePath, "file:") {
		q.DatabasePath = fmt.Sprintf("file:%s", q.DatabasePath)
	}

	params := "?_txlock=immediate&_journal_mode=wal"

	if q.DB == nil {
		db, err := sql.Open("sqlite3", fmt.Sprintf("%s%s", q.DatabasePath, params))
		if err != nil {
			return q, errors.Annotate(err, "opening the database at "+q.DatabasePath)
		}

		q.DB = db
	}

	if q.Clock == nil {
		q.Clock = clock.New()
	}

	return q, nil
}

type SqliteQueue struct {
	params *Params

	// stmts caches our perpared statements
	stmts preparedStatements

	// closeCh is used to signal the cleanup go routines to stop
	closeCh chan struct{}
}

type preparedStatements map[preparedStatement]*sql.Stmt

func (ps preparedStatements) Get(s preparedStatement) *sql.Stmt {
	return ps[s]
}

// New creates a new Queue
func New(params Params) (Queue, error) {
	params, err := params.Defaults()
	if err != nil {
		return nil, errors.Trace(err)
	}

	q := SqliteQueue{
		params: &params,
	}

	err = q.setup()
	if err != nil {
		return nil, errors.Annotate(err, "running setup()")
	}

	return &q, nil
}

func (q *SqliteQueue) setup() error {
	q.closeCh = make(chan struct{})

	tx, err := q.params.DB.Begin()
	if err != nil {
		return errors.Annotate(err, "running setup() Begin()")
	}

	// https://www.sqlite.org/pragma.html#pragma_journal_mode
	// TODO: should probably make these configurable
	tx.Exec("PRAGMA journal_mode = 'WAL'")
	tx.Exec("PRAGMA synchronous = 1;")
	tx.Exec("PRAGMA temp_store = 2;")
	tx.Exec("PRAGMA cache_size = 100000;")

	query := `
        CREATE TABLE IF NOT EXISTS queue (
          job_namespace            TEXT NOT NULL,               /* namespace of the job */
          job_data                 BLOB NOT NULL,               /* json encoded data */
          job_status               INTEGER NOT NULL,            /* 0 = ready, 1 = locked, 2 = done, 3 = failed */
          job_created_at           INTEGER NOT NULL,            /* unix timestamp */
          job_locked_at            INTEGER,                     /* unix timestamp */
          job_finished_at          INTEGER,                     /* unix timestamp */
          job_retries              INTEGER NOT NULL DEFAULT 0,  /* number of times this job has been retried */
          job_scheduled_at         INTEGER NOT NULL DEFAULT 0,  /* unix timestamp */
          job_ttl                  INTEGER NOT NULL DEFAULT 0   /* time to live in seconds */
        )
    `
	_, err = tx.Exec(query)
	if err != nil {
		return errors.Annotate(err, "creating the queue table")
	}

	_, err = tx.Exec(`
        CREATE INDEX IF NOT EXISTS
            queue_namespace_status_scheduled_created_idx
        ON queue(
                job_namespace,
                job_status,
                job_scheduled_at,
                job_created_at
        )
    `)
	if err != nil {
		return errors.Annotate(err, "creating index")
	}

	err = tx.Commit()
	if err != nil {
		return errors.Annotate(err, "committing the transaction")
	}

	preparedStatements := map[preparedStatement]string{
		enqueueStatement: `
            INSERT INTO
                queue(
                    job_namespace,
                    job_data,
                    job_status,
                    job_created_at,
                    job_locked_at,
                    job_finished_at,
                    job_scheduled_at,
                    job_ttl
                )
                VALUES (
                    ?,       /* namespace    */
                    ?,       /* data         */
                    ?,       /* status       */
                    ?,       /* created_at   */
                    NULL,    /* locked_at    */
                    NULL,    /* finished_at  */
                    ?,       /* scheduled_at */
                    ?        /* ttl */
                );`,
		dequeueStatement: `
            UPDATE
                queue
            SET
                job_status = ?,
                job_locked_at = ?
             WHERE rowid = (
                 SELECT
                    rowid
                 FROM
                    queue
                 WHERE
                    job_namespace = ?
                    AND job_status = ?
                    AND job_scheduled_at <= ?
                    AND strftime('%s', 'now') - job_created_at <= job_ttl
                 ORDER BY job_created_at ASC
             )
             RETURNING rowid, *;`,
		updateStatusStatement: `
            UPDATE
                queue
            SET
                job_status = ?,
                job_finished_at = ?
            WHERE
                rowid = ?`,
		updateStatusRetryStatement: `
            UPDATE queue
                SET
                job_status = ?,
                job_finished_at = ?,
                job_retries = job_retries + 1
             WHERE rowid = ?;`,
	}

	q.stmts = make(map[preparedStatement]*sql.Stmt, len(preparedStatements))
	for name, query := range preparedStatements {
		s, err := q.params.DB.Prepare(query)
		if err != nil {
			return errors.Annotate(err, "preparing statements: "+query)
		}
		q.stmts[name] = s
	}

	q.cleanup()

	return nil
}

// EnqueueParams are passed into the Queue.Enqueue method
type EnqueueParams struct {
	// Namespace is the namespace to enqueue the job to
	Namespace string

	// ScheduleAfter is the number of seconds to wait before making the job available
	// for consumption
	ScheduleAfter time.Duration

	// TTL is the number of seconds to keep the job around available for consumption
	TTL time.Duration
}

func (p EnqueueParams) Defaults() EnqueueParams {
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	return p
}

func (q *SqliteQueue) Enqueue(data any, params EnqueueParams) (int64, error) {
	params = params.Defaults()

	res, err := q.stmts.
		Get(enqueueStatement).
		Exec(params.Namespace, data, JobStatusReady, q.params.Clock.Now().Unix(), params.ScheduleAfter, params.TTL)

	if err != nil {
		return 0, errors.Annotate(err, "calling Put()")
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, errors.Trace(err)
	}

	return id, nil
}

type DequeueParams struct {
	// Namespace is the namespace to dequeue from
	Namespace string
}

func (p DequeueParams) Defaults() DequeueParams {
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	return p
}

func (q *SqliteQueue) Dequeue(params DequeueParams) (*Message, error) {
	params = params.Defaults()

	var delay, lockTime, doneTime sql.NullInt64
	var message Message
	err := q.stmts.
		Get(dequeueStatement).
		QueryRow(JobStatusLocked, q.params.Clock.Now().Unix(), params.Namespace, JobStatusReady, q.params.Clock.Now().Unix()).
		Scan(
			&message.ID, &message.Namespace, &message.Data,
			&message.Status, &delay, &lockTime,
			&doneTime, &message.Retries, &message.ScheduledAt, &message.TTL,
		)
	if err != nil && errors.Cause(err) != sql.ErrNoRows {
		return nil, errors.Trace(err)
	} else if errors.Cause(err) == sql.ErrNoRows {
		return nil, nil
	}

	return &message, nil
}

// Done marks the job as done
func (q *SqliteQueue) Done(id int64) error {
	return q.setStatus(id, JobStatusDone)
}

// Fail marks the job as failed
func (q *SqliteQueue) Fail(id int64) error {
	return q.setStatus(id, JobStatusFailed)
}

// Retry marks the message as ready to be consumed again
func (q *SqliteQueue) Retry(id int64) error {
	return q.setStatus(id, JobStatusReady)
}

// Retry marks the message as ready to be consumed again
func (q *SqliteQueue) Size() (int, error) {
	row := q.params.DB.QueryRow(
		fmt.Sprintf(`SELECT COUNT(*) as qsize FROM queue WHERE job_status = %d`, JobStatusReady))

	var queueSize int
	if err := row.Scan(&queueSize); err != nil {
		return 0, errors.Trace(err)
	}

	return queueSize, nil
}

func (q *SqliteQueue) Prune() error {
	_, err := q.params.DB.Exec(
		fmt.Sprintf(`
            DELETE FROM
                queue
            WHERE
                job_status
            IN (%d, %d)
        `, JobStatusDone, JobStatusFailed),
	)

	return errors.Trace(err)
}

func (q *SqliteQueue) Vacuum() error {
	_, err := q.params.DB.Exec("VACUUM")
	return errors.Trace(err)
}

func (q *SqliteQueue) Close() error {
	for _, s := range q.stmts {
		err := s.Close()
		if err != nil {
			return errors.Trace(err)
		}
	}

	close(q.closeCh)

	return nil
}

func (q *SqliteQueue) setStatus(id int64, status JobStatus) error {
	var doneTime int64

	stmt := q.stmts.Get(updateStatusStatement)

	if status != JobStatusReady {
		doneTime = q.params.Clock.Now().Unix()
	} else {
		stmt = q.stmts.Get(updateStatusRetryStatement)
	}

	_, err := stmt.Exec(status, doneTime, id)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// if auto vacuum is enabled (vs you taking care of it manually) by manually calling
// Queue.Vacuum, this spawns a new go routine and vacuums the DB at a set interval
func (q *SqliteQueue) cleanup() {
	q.autovacuum()
	q.autoprune()
}

func (q *SqliteQueue) autovacuum() {
	if q.params.AutoVacuum {
		var ticker *clock.Ticker
		if q.params.AutoVacuumInterval != 0 {
			ticker = q.params.Clock.Ticker(q.params.AutoVacuumInterval)
		} else {
			ticker = q.params.Clock.Ticker(10 * time.Hour)
		}

		go func() {
			for {
				select {
				case <-ticker.C:
					err := q.Vacuum()
					log.Println(err)
				case <-q.closeCh:
					ticker.Stop()
					return
				}
			}
		}()
	}
}
func (q *SqliteQueue) autoprune() {
	if q.params.AutoPrune {
		var ticker *clock.Ticker
		if q.params.AutoPruneInterval != 0 {
			ticker = q.params.Clock.Ticker(q.params.AutoPruneInterval)
		} else {
			ticker = q.params.Clock.Ticker(10 * time.Hour)
		}

		go func() {
			for {
				select {
				case <-ticker.C:
					err := q.Prune()
					log.Println(err)
				case <-q.closeCh:
					ticker.Stop()
					return
				}
			}
		}()
	}
}
