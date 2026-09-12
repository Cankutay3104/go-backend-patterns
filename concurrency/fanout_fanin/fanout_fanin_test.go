package fanoutfanin_test

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	fanoutfanin "go-backend-patterns/concurrency/fanout_fanin"
	"go-backend-patterns/internal/leaktest"
)

// Helper to push values to a channel and close it.
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

// Verifies that Merge combines multiple input streams into a single output stream with zero data loss.
func TestMerge_AllValuesReceived(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch1 := produce(1, 2, 3)
	ch2 := produce(4, 5, 6)
	ch3 := produce(7, 8, 9)

	merged := fanoutfanin.Merge(ctx, ch1, ch2, ch3)

	var results []int
	for val := range merged {
		results = append(results, val)
	}

	if len(results) != 9 {
		t.Fatalf("expected 9 items, got %d", len(results))
	}

	// Because goroutines interleave non-deterministically (not in a sorted order), therefore; we sort before asserting.
	sort.Ints(results)
	for i, v := range results {
		if v != i+1 {
			t.Errorf("expected index %d to be %d, got %d", i, i+1, v)
		}
	}
}

// Verifies that N workers process items concurrently.
func TestFanOut_ParallelThroughput(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const totalItems = 6
	const sleepPerItem = 40 * time.Millisecond
	const numWorkers = 3

	in := produce(1, 2, 3, 4, 5, 6)

	start := time.Now()

	// Slow transform to simulate CPU or I/O load
	transform := func(n int) int {
		time.Sleep(sleepPerItem)
		return n * 10
	}

	out := fanoutfanin.FanOut(ctx, in, numWorkers, transform)

	var results []int
	for res := range out {
		results = append(results, res)
	}

	elapsed := time.Since(start)

	if len(results) != totalItems {
		t.Fatalf("expected %d results, got %d", totalItems, len(results))
	}

	// 6 items at 40ms sequential = 240ms.
	// With 3 workers, it should take ~80ms (plus small scheduling overhead).
	// We check that elapsed is well below the sequential threshold (e.g. < 180ms).
	if elapsed >= 200*time.Millisecond {
		t.Errorf("fan-out took %v; expected parallel execution under 200ms", elapsed)
	}
}

// Ensures that canceling the context during active worker processing cleans up all internal worker and merger goroutines.
func TestFanOut_CancellationLeakFree(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int)
	var wg sync.WaitGroup

	// Stream that produces until context cancellation
	wg.Go(func() {
		defer close(in)
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			case in <- i:
			}
		}
	})

	transform := func(n int) int {
		return n * 2
	}

	out := fanoutfanin.FanOut(ctx, in, 4, transform)

	// Read 5 items then cancel downstream
	readCount := 0
	for range out {
		readCount++
		if readCount == 5 {
			cancel()
			break
		}
	}

	wg.Wait()
	time.Sleep(20 * time.Millisecond)
}

// Ensures that an empty closed input channel terminates immediately.
func TestFanOut_EmptyInput(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int)
	close(in) // Immediately closed

	out := fanoutfanin.FanOut(ctx, in, 3, func(n int) int { return n })

	count := 0
	for range out {
		count++
	}

	if count != 0 {
		t.Fatalf("expected 0 items from empty channel, got %d", count)
	}
}
