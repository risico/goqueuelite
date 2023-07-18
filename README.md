<p align="center">
<h1>squeuelite</h1>
<img width="400px" src="https://i.imgur.com/vk0HXPE.png" alt="squeuelito" />
Tiny little queue on top of SQLite written in Go.
</p>

sQueueLite is a simplistic, SQLite backed job embedded queue library for Go applications.
It provides an easy way to manage and process background jobs, facilitating the scheduling and processing of tasks in an organized and efficient manner.

> **Warning**
> Is still in heavy development and the API is not finalized yet and might change any moment.


## Features

# Installation
```
go get github.com/risico/squeuelite
```

# Usage

```go
package main

import "github.com/risico/squeuelite"

func main() {
	params := squeuelite.Params{
		DatabasePath: "queue.db",
		AutoVacuum:   true,
		AutoPrune:    true,
	}
	queue, err := squeuelite.New(params)
	if err != nil {
		panic(err)
	}
	defer queue.Close()
}
```

## Enqueuing a Job
```go
data := "Your job data here"
id, err := queue.Enqueue(data)
if err != nil {
	// handle error
}
```

## Dequeueing a Job
```go
namespace := "default"
message, err := queue.Dequeue(namespace)
if err != nil {
	// handle error
}
```

## Marking a Job as Done or Failed
```go
err = queue.Done(jobID)
if err != nil {
	// handle error
}

err = queue.Fail(jobID)
if err != nil {
	// handle error
}
```

## Retrying a job
```go
err = queue.Retry(jobID)
if err != nil {
	// handle error
}
```

## Getting the size of the queue
```go
size, err := queue.Size()
if err != nil {
	// handle error
}
```

## Checking if the queue is empty
```go
isEmpty, err := queue.Empty()
if err != nil {
	// handle error
}
```

## Manual Pruning and Vacuuming
If you have disabled AutoPrune and AutoVacuum, you can manually prune and vacuum the database.

```go
err = queue.Prune()
if err != nil {
	// handle error
}

err = queue.Vacuum()
if err != nil {
	// handle error
}
```


### Contributing
Feel free to contribute to this project by opening issues or submitting pull requests for bug fixes or features.

### License
This project is licensed under the MIT License.
