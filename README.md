<p align="center">
<h1>GoQueueLite</h1>
<img width="400px" src="https://i.imgur.com/vk0HXPE.png" alt="goqueuelito" />
    Tiny little queue on top of SQLite written in Go.
</p>

sQueueLite is a simplistic, SQLite backed job embedded queue library for Go applications.
It provides an easy way to manage and process background jobs, facilitating the scheduling and processing of tasks in an organized and efficient manner.

> **Warning**
> Is still in heavy development and the API is not finalized yet and might change any moment.


## Features

# Installation
```
go get github.com/risico/goqueuelite
```

# Usage

```go
package main

import "github.com/risico/goqueuelite"

func main() {
	params := goqueuelite.Params{
		DatabasePath: "queue.db",
		AutoVacuum:   true,
		AutoPrune:    true,
	}
	queue, err := goqueuelite.New(params)
	if err != nil {
		panic(err)
	}
	defer queue.Close()
}
```

## Enqueuing a Job
```go
data := "Your job data here"
params := goqueuelite.EnqueueParams{
    Namespace: "test_namespace",
    ScheduleAfter: time.Now().Add(1 * time.Hour),
    TTL: 2 * time.Hour,
}
id, err := queue.Enqueue(data, params)
if err != nil {}
```

## Dequeueing a Job
```go
params := goqueuelite.DequeueParams{
    Namespace: "default",
}
message, err := queue.Dequeue(params)
if err != nil {}
fmt.Printf("Message(%+v) \n", message)
```

### Message Payload
```go
type Message struct {
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
```

## Marking a Job as Done or Failed
```go
err = queue.Done(messageID)
if err != nil {
	// handle error
}

err = queue.Fail(messageID)
if err != nil {
	// handle error
}
```

## Retrying a job
```go
err = queue.Retry(messageID)
if err != nil {
	// handle error
}
```

## Getting the size of the queue
```go
size, err := queue.Size()
```

## Checking if the queue is empty
```go
isEmpty, err := queue.Empty()
```

## Manual Pruning and Vacuuming
If you have disabled AutoPrune and AutoVacuum, you can manually prune and vacuum the database.

```go
queue.Prune()
queue.Vacuum()
```


### Contributing
Feel free to contribute to this project by opening issues or submitting pull requests for bug fixes or features.

### License
This project is licensed under the MIT License.
