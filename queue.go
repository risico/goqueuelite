package squeuelite

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/juju/errors"
	_ "github.com/mattn/go-sqlite3"
)

type JobStatus int

const (
	JobStatusReady  JobStatus = 0
	JobStatusLocked JobStatus = 1
	JobStatusDone   JobStatus = 2
	JobStatusFailed JobStatus = 3
)

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
	Data        string
	Namespace   string
	Status      JobStatus
	Delay       uint64
	LockTime    int
	DoneTime    int
	Retries     int
	ScheduledAt int
}

// Params are passed into the Queue and accept external user input
type Params struct {
	// DB is the main link to the database, you can either pass this from outside
	// or if left nil it will try to create it
	DB *sql.DB

	// DatabasePath is the path where the database sits (if no sql.DB is being passed)
	DatabasePath string

	// AutoVacuum automatically handles vaccuming the db, if this is not
	// enabled you will have to take care of it by manually calling Queue.Vacuum
	AutoVacuum         bool
	AutoVacuumInterval time.Duration

	// AutoPrune deletes completed jobs
	AutoPrune         bool
	AutoPruneInterval time.Duration

	// MaxRetries will automatically mark the message as failed if it was retried
	// N times
	MaxRetries int
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

	return q, nil
}

type SqliteQueue struct {
	params *Params

	// stmts caches our perpared statements
	stmts preparedStatements

	closeCh chan struct{}
}

type preparedStatements map[preparedStatement]*sql.Stmt

func (ps preparedStatements) Get(s preparedStatement) *sql.Stmt {
	return ps[s]
}

func New(params Params) (*SqliteQueue, error) {
	params, err := params.Defaults()
	if err != nil {
		return nil, errors.Trace(err)
	}

	q := &SqliteQueue{
		params: &params,
	}

	err = q.setup()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return q, nil
}

func (q *SqliteQueue) setup() (err error) {
	q.closeCh = make(chan struct{})

	tx, err := q.params.DB.Begin()
	if err != nil {
		return errors.Annotate(err, "running setup() Begin()")
	}

	// roll back the transaction on errors
	defer func() {
		if err != nil {
			err = errors.Wrap(err, tx.Rollback())
		}
	}()

	tx.Exec("PRAGMA journal_mode = 'WAL'")
	tx.Exec("PRAGMA synchronous = 1;")
	tx.Exec("PRAGMA temp_store = 2;")
	tx.Exec("PRAGMA cache_size=100000;")

	query := `
        CREATE TABLE IF NOT EXISTS queue (
          job_namespace            TEXT NOT NULL,
          job_data                 BLOB NOT NULL,
          job_status               INTEGER NOT NULL,
          job_created_at           INTEGER NOT NULL,
          job_locked_at            INTEGER,
          job_finished_at          INTEGER,
          job_retries              INTEGER NOT NULL DEFAULT 0,
          job_scheduled_at         INTEGER NOT NULL DEFAULT 0
        )
    `
	tx.Exec(query)
	tx.Exec("CREATE INDEX IF NOT EXISTS queue_nm_sts_sched_idx ON queue(job_namespace, job_status, job_scheduled_at)")

	err = tx.Commit()
	if err != nil {
		return errors.Annotate(err, "running setup transactions")
	}

	preparedStatements := map[preparedStatement]string{
		enqueueStatement: `INSERT INTO
            queue(job_namespace, job_data, job_status, job_created_at, job_locked_at, job_finished_at, job_scheduled_at)
            VALUES (?,?,?,?,NULL,NULL,?);`,
		dequeueStatement: `
             UPDATE queue SET job_status = ?, job_locked_at = ?
             WHERE rowid = (
                 SELECT rowid FROM queue
                 WHERE job_namespace = ? AND job_status = ? AND job_scheduled_at <= ?
             )
             RETURNING rowid,*;`,
		updateStatusStatement: `
            UPDATE queue SET job_status = ?, job_finished_at = ? WHERE rowid = ?`,
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

type EnqueueParams struct {
	Namespace     string
	ScheduleAfter time.Duration
}

func (p EnqueueParams) Defaults() EnqueueParams {
	if p.Namespace == "" {
		p.Namespace = "default"
	}

	return p
}

func (q *SqliteQueue) Enqueue(data any) (int64, error) {
	return q.EnqueueWithParams(data, EnqueueParams{})
}

func (q *SqliteQueue) EnqueueWithParams(data any, params EnqueueParams) (int64, error) {
	params = params.Defaults()
	res, err := q.stmts.
		Get(enqueueStatement).
		Exec(params.Namespace, data, JobStatusReady, time.Now().Unix(), params.ScheduleAfter)

	if err != nil {
		return 0, errors.Annotate(err, "calling Put()")
	}

	id, err := res.LastInsertId()
	if err != nil {
		return 0, errors.Trace(err)
	}

	return id, nil
}

func (q *SqliteQueue) Dequeue(namespace string) (*Message, error) {
	var delay, lockTime, doneTime sql.NullInt64

	var message Message
	err := q.stmts.
		Get(dequeueStatement).
		QueryRow(JobStatusLocked, time.Now().Unix(), namespace, JobStatusReady, time.Now().Unix()).
		Scan(&message.ID, &message.Namespace, &message.Data, &message.Status, &delay, &lockTime, &doneTime, &message.Retries, &message.ScheduledAt)
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
		fmt.Sprintf(`SELECT COUNT(*) as qsize FROM queue WHERE job_status NOT IN (%d, %d, %d)`,
			JobStatusDone, JobStatusFailed, JobStatusLocked))

	var queueSize int
	if err := row.Scan(&queueSize); err != nil {
		return 0, errors.Trace(err)
	}

	return queueSize, nil
}

func (q *SqliteQueue) Empty() (bool, error) {
	row := q.params.DB.QueryRow(
		fmt.Sprintf(`SELECT COUNT(*) as qsize FROM queue WHERE job_status = %d`,
			JobStatusReady))

	var queueSize bool
	if err := row.Scan(&queueSize); err != nil {
		return false, errors.Trace(err)
	}

	return queueSize, nil
}

func (q *SqliteQueue) Prune() error {
	_, err := q.params.DB.Exec(
		fmt.Sprintf(`DELETE FROM queue WHERE job_status IN (%d, %d)`,
			JobStatusDone, JobStatusFailed))

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
		doneTime = time.Now().Unix()
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
		var ticker *time.Ticker
		if q.params.AutoVacuumInterval != 0 {
			ticker = time.NewTicker(q.params.AutoVacuumInterval)
		} else {
			ticker = time.NewTicker(10 * time.Hour)
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
		var ticker *time.Ticker
		if q.params.AutoPruneInterval != 0 {
			ticker = time.NewTicker(q.params.AutoPruneInterval)
		} else {
			ticker = time.NewTicker(10 * time.Hour)
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
