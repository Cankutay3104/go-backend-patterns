# concurrency/pipeline

A composable, streaming data-processing pipeline built using Go generics and standard library concurrency primitives.

---

## The Problem

Traditional batch processing aggregates intermediate results into discrete slices between successive operational stages:

```go
records := parseAll(rawData)       // Allocates O(n) memory
filtered := filterAll(records)     // Allocates O(n) memory
hashed := hashAll(filtered)        // Allocates O(n) memory
```

This design exhibits two major architectural failure modes under high load:

1. **Memory Inflation ($O(n)$ footprint):** Retaining full datasets in memory strains the garbage collector and introduces Out-Of-Memory (OOM) failure risks during volume spikes.
2. **Batch Latency Degradation:** Downstream processing remains idle until upstream completely processes the final entry in the source batch.

---

## Architectural Mechanics

A streaming pipeline decouples processing stages using unbuffered channels. Stages execute concurrently in isolated goroutines, handing over data items sequentially.

```text
[ Generate (Source) ] ──chan──► [ Filter (Stage) ] ──chan──► [ Map (Stage) ] ──chan──► [ Consumer (Sink) ]
```

### Core Invariants

1. **Single Channel Ownership:** Every stage owns the channel it produces. The owning stage runs an asynchronous worker goroutine and guarantees closure via `defer close(out)`.
2. **Guarded Cancellation on Read and Write:** To avoid goroutine leaks when downstream consumers terminate early, every channel receive and send operation is multiplexed with a cancellation check:
   ```go
   select {
   case <-ctx.Done():
       return
   case out <- item:
   }
   ```
3. **Bounded Memory Overhead ($O(1)$ space):** Unbuffered channels ensure that only items actively undergoing computation exist in flight, keeping memory usage constant regardless of input volume.
4. **Type-Safe Transformations:** The pipeline utilizes Go generics (`[I, O any]`) to provide compile-time type verification without allocations or interface casting.

---

## Stage Primitives

* **`Generate[T any](ctx, values ...T) <-chan T`:** Converts a discrete collection of inputs into an asynchronous receive-only stream.
* **`Filter[T any](ctx, in <-chan T, predicate func(T) bool) <-chan T`:** Evaluates items from an input stream and selectively propagates only those satisfying the predicate.
* **`Map[I, O any](ctx, in <-chan I, transform func(I) O) <-chan O`:** Ingests items of type `I`, computes a transformation, and emits items of type `O`.

---

## Usage

```go
package main

import (
    "context"
    "fmt"

    "go-backend-patterns/concurrency/pipeline"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 1. Ingest inputs into an asynchronous stream
    numbers := pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    // 2. Filter out non-matching elements
    evens := pipeline.Filter(ctx, numbers, func(n int) bool {
        return n%2 == 0
    })

    // 3. Transform filtered elements into output models
    squaredStrings := pipeline.Map(ctx, evens, func(n int) string {
        return fmt.Sprintf("Result: %d", n*n)
    })

    // 4. Drain the stream at the consumer sink
    for output := range squaredStrings {
        fmt.Println(output)
    }
}
```

---

## Verification

Execute the test suite under the Go race detector with runtime goroutine leak assertions:

```bash
go test -v -race ./concurrency/pipeline/...
```