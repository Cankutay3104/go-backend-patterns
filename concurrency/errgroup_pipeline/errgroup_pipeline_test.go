package errgrouppipeline_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	errgrouppipeline "go-backend-patterns/concurrency/errgroup_pipeline"
	"go-backend-patterns/internal/leaktest"
)

// Helper function to produce an input channel populated with discrete values.
func produce[T any](values ...T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, val := range values {
			out <- val
		}
	}()
	return out
}

// Verifies that a valid stream processes completely and wait() returns nil.
func TestParallelMap_Success(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := produce(1, 2, 3, 4, 5, 6)

	transform := func(ctx context.Context, n int) (int, error) {
		return n * 2, nil
	}

	out, wait := errgrouppipeline.ParallelMap(ctx, input, 3, transform)

	var results []int
	for val := range out {
		results = append(results, val)
	}

	if err := wait(); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if len(results) != 6 {
		t.Fatalf("expected 6 results, got: %d", len(results))
	}

	sort.Ints(results)
	expected := []int{2, 4, 6, 8, 10, 12}
	for i, v := range results {
		if v != expected[i] {
			t.Errorf("at index %d: expected %d, got %d", i, expected[i], v)
		}
	}
}

// Verifies that returning an error from a worker cancels the context across all siblings and wait() surfaces that exact error.
func TestParallelMap_FailFastError(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errBomb := errors.New("simulated database failure")

	// Create an unbounded stream of integers
	in := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(in)
		for i := 1; ; i++ {
			select {
			case <-ctx.Done():
				return
			case in <- i:
			}
		}
	}()

	transform := func(workerCtx context.Context, n int) (int, error) {
		// Trigger an error once item 3 arrives
		if n == 3 {
			return 0, errBomb
		}
		return n * 10, nil
	}

	out, wait := errgrouppipeline.ParallelMap(ctx, in, 4, transform)

	// Consume whatever is emitted until out closes due to error teardown
	for range out {
	}

	// Verify that wait() returned our target error
	err := wait()
	if !errors.Is(err, errBomb) {
		t.Fatalf("expected error %v, got %v", errBomb, err)
	}

	// Cancel the root context to unblock our unbounded generator
	cancel()
	wg.Wait()
	time.Sleep(20 * time.Millisecond)
}

// Verifies that canceling the parent context terminates workers and cleaner goroutine cleanly.
func TestParallelMap_ContextCancellationLeakFree(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(in)
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case in <- i:
			}
		}
	}()

	transform := func(ctx context.Context, n int) (int, error) {
		return n, nil
	}

	out, wait := errgrouppipeline.ParallelMap(ctx, in, 3, transform)

	// Read 3 items, then cancel the pipeline context
	readCount := 0
	for range out {
		readCount++
		if readCount == 3 {
			cancel()
			break
		}
	}

	// Drain remaining items so out can close cleanly
	for range out {
	}

	_ = wait()

	wg.Wait()
	time.Sleep(20 * time.Millisecond)
}

// Verifies that an empty closed input channel terminates immediately without hanging.
func TestParallelMap_EmptyInput(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int)
	close(in)

	out, wait := errgrouppipeline.ParallelMap(ctx, in, 2, func(ctx context.Context, n int) (int, error) {
		return n, nil
	})

	count := 0
	for range out {
		count++
	}

	if count != 0 {
		t.Fatalf("expected 0 items from empty channel, got: %d", count)
	}

	if err := wait(); err != nil {
		t.Fatalf("expected nil error on empty channel, got: %v", err)
	}
}
