package goqueuelite_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/risico/clock"

	"github.com/risico/goqueuelite"
)

func TestGoqueueLite(t *testing.T) {
	t.Parallel()
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	id, err := q.Enqueue("something", goqueuelite.EnqueueParams{})
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	message, err := q.Dequeue(goqueuelite.DequeueParams{})
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
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
		Clock:        mClock,
	})
	assert.NoError(t, err)

    // Enque an item that has a TTL of 10 seconds
	id, err := q.Enqueue("something", goqueuelite.EnqueueParams{
		TTL: 10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, id)

    // It should receive the item
	message, err := q.Dequeue(goqueuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.NotNil(t, message)
	if message != nil {
		err = q.Done(id)
		assert.NoError(t, err)
	}


	_, err = q.Enqueue("something", goqueuelite.EnqueueParams{
		TTL: 10 * time.Second,
	})
	assert.NoError(t, err)

    // Advance the clock by 10 seconds
    mClock.Add(12 * time.Second)
	message, err = q.Dequeue(goqueuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.Nil(t, message)

	q.Close()
}

func TestGoqueueLiteSize(t *testing.T) {
	t.Parallel()
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	size, err := q.Size()
	assert.NoError(t, err)
	assert.Equal(t, 0, size)

	_, err = q.Enqueue("something", goqueuelite.EnqueueParams{})
	assert.NoError(t, err)

	size, err = q.Size()
	assert.NoError(t, err)
	assert.Equal(t, 1, size)

	q.Close()
}


func TestGoqueueLiteSubscribe(t *testing.T) {
	t.Parallel()
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

    ch, err := q.Subscribe("default")
	assert.NoError(t, err)
    assert.NotNil(t, ch)

	_, err = q.Enqueue("default", goqueuelite.EnqueueParams{})
	assert.NoError(t, err)

    m := <-ch
    assert.NotNil(t, m)
    assert.NotZero(t, m.MessageID)
    assert.Equal(t, m.Namespace, "default")

    mm, err := q.Lock(m.MessageID)
    assert.NoError(t, err)
    assert.Equal(t, m.MessageID, mm.ID)

	q.Close()
}

func TestGoqueueLiteDequeue(t *testing.T) {
	t.Parallel()
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	message, err := q.Dequeue(goqueuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.Nil(t, message)

	_, err = q.Enqueue("something", goqueuelite.EnqueueParams{})
	assert.NoError(t, err)

	message, err = q.Dequeue(goqueuelite.DequeueParams{})
	assert.NoError(t, err)
	assert.NotNil(t, message)

	q.Close()
}

func TestGoqueueLiteDone(t *testing.T) {
	t.Parallel()
	q, err := goqueuelite.New(goqueuelite.Params{
		DatabasePath: ":memory:",
	})
	assert.NoError(t, err)

	id, err := q.Enqueue("something", goqueuelite.EnqueueParams{})
	assert.NoError(t, err)

	err = q.Done(id)
	assert.NoError(t, err)

	q.Close()
}

// func TestGoqueueLiteLoad(t *testing.T) {
// 	t.Parallel()
// 	defer os.Remove("test.db")
// 	q, err := goqueuelite.New(goqueuelite.Params{
// 		DatabasePath: "test.db",
// 	})
// 	assert.NoError(t, err)
//
// 	for x := 0; x < 100_000; x++ {
// 		_, err := q.Enqueue(x, goqueuelite.EnqueueParams{})
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
// 				m, err := q.Dequeue(goqueuelite.DequeueParams{})
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
