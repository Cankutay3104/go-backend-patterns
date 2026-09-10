package pipeline_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"go-backend-patterns/concurrency/pipeline"
	"go-backend-patterns/internal/leaktest"
)

// Verifies that streaming items flow sequentially through Generate, Filter, and Map stages to produce the expected final slice.
func TestPipeline_EndToEnd(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Stream numbers 1 through 6
	sourceStream := pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6)

	// Keep only even numbers
	evenFilter := func(n int) bool {
		return n%2 == 0
	}
	filteredStream := pipeline.Filter(ctx, sourceStream, evenFilter)

	// Multiply remaining numbers by 10
	multiplyByTen := func(n int) int {
		return n * 10
	}
	transformedStream := pipeline.Map(ctx, filteredStream, multiplyByTen)

	// Collect all transformed items
	var results []int
	for val := range transformedStream {
		results = append(results, val)
	}

	expected := []int{20, 40, 60}
	if !reflect.DeepEqual(results, expected) {
		t.Fatalf("pipeline output mismatch: expected %v, got %v", expected, results)
	}
}

// Verifies that canceling the pipeline context while stages are actively processing cleanly terminates all background goroutines.
func TestPipeline_CancellationPreventsLeaks(t *testing.T) {
	defer leaktest.Check(t)()

	// Create a context that will be manually canceled early
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate a continuous stream of data
	infiniteValues := make([]int, 1000)
	for i := range infiniteValues {
		infiniteValues[i] = i
	}
	sourceStream := pipeline.Generate(ctx, infiniteValues...)

	// Transform elements
	mappedStream := pipeline.Map(ctx, sourceStream, func(n int) int {
		return n * 2
	})

	// Abandon the stream after reading only 3 items
	itemsRead := 0
	for range mappedStream {
		itemsRead++
		if itemsRead == 3 {
			// Trigger early cancellation; downstream stops consuming
			cancel()
			break
		}
	}

	// Allow a brief moment for context cancellation signals to spread across stages
	time.Sleep(20 * time.Millisecond)
}

// Verifies that an empty generator terminates cleanly without hanging or throwing unexpected values.
func TestPipeline_EmptyInput(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceStream := pipeline.Generate[int](ctx)
	filteredStream := pipeline.Filter(ctx, sourceStream, func(n int) bool { return true })
	mappedStream := pipeline.Map(ctx, filteredStream, func(n int) int { return n })

	count := 0
	for range mappedStream {
		count++
	}

	if count != 0 {
		t.Fatalf("expected 0 items from empty stream, got: %d", count)
	}
}
