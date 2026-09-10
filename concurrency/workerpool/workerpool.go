package workerpool

import (
	"context"
	"errors"
	"sync"
)

// Standard sentinel errors returned when operations fail or are rejected.
var (
	ErrPoolStopped     = errors.New("workerpool: submission rejected, pool is closed")
	ErrInvalidPoolSize = errors.New("workerpool: worker count must be greater than zero")
)

// Task represents a unit of work that can be canceled by passing a context.
type Task func(ctx context.Context)

// Pool manages a fixed group of workers that pull tasks from a shared channel.
type Pool struct {
	workerCount int
	taskQueue   chan Task

	// Controls access to the closed state so that workers and callers do not race.
	lifecycleMutex sync.RWMutex
	isClosed       bool

	// Allows the pool to cancel all running workers at the same time.
	poolContext   context.Context
	cancelWorkers context.CancelFunc

	// Tracks active workers so the pool knows when every worker has fully exited.
	workerWaitGroup sync.WaitGroup
}

func New(workerCount int, queueCapacity int) (*Pool, error) {
	if workerCount <= 0 {
		return nil, ErrInvalidPoolSize
	}

	// Creating a root context that allows us to cancel all workers later.
	ctx, cancelFunc := context.WithCancel(context.Background())

	pool := &Pool{
		poolContext:   ctx,
		cancelWorkers: cancelFunc,
		taskQueue:     make(chan Task, queueCapacity),
		workerCount:   workerCount,
	}

	// Starting all workers
	pool.dispatchWorkers()

	return pool, nil
}

// Helper function for New constructor, which starts all worker goroutines and registers them with the wait group.
func (pool *Pool) dispatchWorkers() {
	// Telling the wait group the amount of workers we want to start.
	pool.workerWaitGroup.Add(pool.workerCount)

	// Starting background worker goroutines.
	for i := 0; i < pool.workerCount; i++ {
		go func() {
			// Decreasing the wait group counter when worker exits.
			defer pool.workerWaitGroup.Done()
			for {
				select {
				// Exit directly if the pool context is canceled.
				case <-pool.poolContext.Done():
					return
				// Waiting for the incoming tasks.
				case task, ok := <-pool.taskQueue:
					if !ok {
						// If channel is closed, tasks ended, worker exits.
						return
					}

					if task != nil {
						// Execute the task and continue the loop.
						task(pool.poolContext)
					}
				}
			}
		}()
	}

}

// Sends a task into the pool queue.
// Returns ErrPoolStopped if the pool is closed, or callerContext.Err() if the caller context times out.
func (pool *Pool) Submit(callerContext context.Context, task Task) error {
	// Handling read access using read lock.
	pool.lifecycleMutex.RLock()
	closed := pool.isClosed
	pool.lifecycleMutex.RUnlock()

	if closed {
		return ErrPoolStopped
	}

	// Waiting to place teh task into the channel without blocking forever.
	select {
	case <-callerContext.Done():
		return callerContext.Err()
	case <-pool.poolContext.Done():
		return ErrPoolStopped
	case pool.taskQueue <- task:
		return nil
	}

}

// Shuts down the pool gracefully.
// It stops accepting new tasks, closes the queue, and blocks until all queued tasks are processed.
func (pool *Pool) Stop() {
	pool.lifecycleMutex.Lock()

	// Return immediately if Stop has already been called before.
	if pool.isClosed {
		pool.lifecycleMutex.Unlock()
		return
	}

	pool.isClosed = true
	pool.lifecycleMutex.Unlock()

	// Closing the channel to signal there are no upcoming tasks.
	close(pool.taskQueue)

	// Block until all the workers finish the queue and return.
	pool.workerWaitGroup.Wait()

	// At last release the workers.
	pool.cancelWorkers()
}
