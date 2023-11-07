package squeuelite_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/risico/clock"

	"github.com/risico/squeuelite"
)

func TestSqueueLite(t *testing.T) {
	t.Parallel()
	q, err := squeuelite.New(squeuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	id, err := q.Enqueue("something", squeuelite.EnqueueParams{})
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	message, err := q.Dequeue(squeuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.NotNil(t, message)

	if message != nil {
		err = q.Done(id)
		assert.NoError(t, err)
	}

	q.Close()
}

func TestTTL(t *testing.T) {
	t.Parallel()

	mClock := clock.NewMock()

	q, err := squeuelite.New(squeuelite.Params{
		DatabasePath: ":memory:",
		Clock:        mClock,
	})
	assert.NoError(t, err)

	id, err := q.Enqueue("something", squeuelite.EnqueueParams{
		TTL: 1 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	message, err := q.Dequeue(squeuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.NotNil(t, message)

	if message != nil {
		err = q.Done(id)
		assert.NoError(t, err)
	}

	message, err = q.Dequeue(squeuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.Nil(t, message)

	q.Close()
}

func TestSqueueLiteSize(t *testing.T) {
	t.Parallel()
	q, err := squeuelite.New(squeuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	size, err := q.Size()
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	_, err = q.Enqueue("something", squeuelite.EnqueueParams{})
	assert.NoError(t, err)

	size, err = q.Size()
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	q.Close()
}

func TestSqueueLiteDequeue(t *testing.T) {
	t.Parallel()
	q, err := squeuelite.New(squeuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	message, err := q.Dequeue(squeuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.Nil(t, message)

	_, err = q.Enqueue("something", squeuelite.EnqueueParams{})
	assert.NoError(t, err)

	message, err = q.Dequeue(squeuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.NotNil(t, message)

	q.Close()
}

func TestSqueueLiteDone(t *testing.T) {
	t.Parallel()
	q, err := squeuelite.New(squeuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	id, err := q.Enqueue("something", squeuelite.EnqueueParams{})
	assert.NoError(t, err)

	err = q.Done(id)
	assert.NoError(t, err)

	q.Close()
}

// func TestSqueueLiteLoad(t *testing.T) {
// 	t.Parallel()
// 	defer os.Remove("test.db")
// 	q, err := squeuelite.New(squeuelite.Params{
// 		DatabasePath: "test.db",
// 	})
// 	assert.NoError(t, err)
//
// 	for x := 0; x < 100_000; x++ {
// 		_, err := q.Enqueue(x, squeuelite.EnqueueParams{})
// 		require.NoError(t, err)
// 	}
//
// 	initialQueueSize, err := q.Size()
// 	assert.NoError(t, err)
// 	assert.Equal(t, 100_000, initialQueueSize)
//
// 	wg := sync.WaitGroup{}
// 	wg.Add(20)
// 	for x := 0; x < 20; x++ {
// 		go func() {
// 			defer wg.Done()
// 			for x := 0; x <= 10_000; x++ {
// 				m, err := q.Dequeue(squeuelite.DequeueParams{})
// 				assert.NoError(t, err)
//
// 				if m == nil {
// 					continue
// 				}
//
// 				err = q.Done(m.ID)
// 				require.NoError(t, err)
// 			}
// 		}()
// 	}
//
// 	wg.Wait()
//
// 	finalQueueSize, err := q.Size()
// 	assert.NoError(t, err)
// 	assert.Equal(t, 0, finalQueueSize)
//
// 	q.Close()
// }
