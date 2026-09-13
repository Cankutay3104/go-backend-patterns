package ordonechannel_test

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	ordonechannel "go-backend-patterns/concurrency/or_done_channel"
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

// Verifies that OrDone passes all values through when the upstream finishes normally.
func TestOrDone_DrainsNormally(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := produce(10, 20, 30, 40)
	safeStream := ordonechannel.OrDone(ctx, source)

	var results []int
	for val := range safeStream {
		results = append(results, val)
	}

	expected := []int{10, 20, 30, 40}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("expected %v, got %v", expected, results)
	}
}

// Verifies that when upstream produces indefinitely, cancelling the context cleanly tears down the OrDone goroutine with zero leaks.
func TestOrDone_ContextCancellationPreventsLeak(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)

	// An infinite source stream
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

	safeStream := ordonechannel.OrDone(ctx, in)

	// Read only 3 items, then cancel the context
	readCount := 0
	for range safeStream {
		readCount++
		if readCount == 3 {
			cancel()
			break
		}
	}

	// Drain any leftover to let everything settle
	for range safeStream {
	}

	wg.Wait()
	time.Sleep(20 * time.Millisecond)
}

// Verifies that an empty closed channel terminates immediately.
func TestOrDone_EmptyInput(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan string)
	close(in)

	safeStream := ordonechannel.OrDone(ctx, in)

	count := 0
	for range safeStream {
		count++
	}

	if count != 0 {
		t.Fatalf("expected 0 items, got %d", count)
	}
}
