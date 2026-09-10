package workerpool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go-backend-patterns/concurrency/workerpool"
	"go-backend-patterns/internal/leaktest"
)

// Verifie that a bounded worker pool executes all submitted tasks and cleanly terminates without leaking goroutines
func TestWorkerPool_ProcessesAllTasks(t *testing.T) {
	// Assert zero leaked goroutines remain after this test exits
	defer leaktest.Check(t)()

	const (
		workerCount   = 4
		queueCapacity = 20
		totalTasks    = 50
	)

	pool, err := workerpool.New(workerCount, queueCapacity)
	if err != nil {
		t.Fatalf("expected nil error on pool initialization, got %v", err)
	}

	var completedCount atomic.Int64

	// Submit tasks concurrently to exercise the pool under load
	for i := 0; i < totalTasks; i++ {
		task := func(ctx context.Context) {
			time.Sleep(5 * time.Millisecond)
			completedCount.Add(1)
		}

		if err := pool.Submit(context.Background(), task); err != nil {
			t.Fatalf("unexpected submission failure on task %d: %v", i, err)
		}
	}

	// Stop waits for all queued tasks to finish before returning.
	pool.Stop()

	// Verify that every single task was executed.
	if completedCount.Load() != int64(totalTasks) {
		t.Fatalf("expected %d completed tasks, got: %d", totalTasks, completedCount.Load())
	}
}

// Verifies that callers receive ErrPoolStopped when attempting to submit tasks after shutdown has begun.
func TestWorkerPool_RejectsSubmissionsAfterStop(t *testing.T) {
	defer leaktest.Check(t)()

	pool, err := workerpool.New(2, 5)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	// Close the pool immediately.
	pool.Stop()

	// Attempting to submit work must fail with ErrPoolStopped.
	err = pool.Submit(context.Background(), func(ctx context.Context) {})
	if !errors.Is(err, workerpool.ErrPoolStopped) {
		t.Fatalf("expected ErrPoolStopped, got: %v", err)
	}
}

// Verifies that when the task queue is saturated, a caller with an expiring context unblocks immediately and receives the context error.
func TestWorkerPool_SubmitContextCancellation(t *testing.T) {
	defer leaktest.Check(t)()

	pool, err := workerpool.New(1, 0)
	if err != nil {
		t.Fatalf("failed to create pool: %v", err)
	}

	// Keep the single worker busy so the queue stays blocked.
	blockWorker := make(chan struct{})

	_ = pool.Submit(context.Background(), func(ctx context.Context) {
		<-blockWorker
	})

	// Create a short-lived context to test caller timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// This submission must block because the queue is full and the worker is busy
	err = pool.Submit(ctx, func(ctx context.Context) {})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	// Unblock the busy worker first so it can finish its task before stopping the pool.
	close(blockWorker)

	pool.Stop()
}
