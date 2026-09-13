# concurrency/or_done_channel

A lightweight utility pattern designed to encapsulate cancellation logic around raw Go channels, preventing goroutine leaks and simplifying stream consumption.

---

## The Problem

Iterating over channels in Go using a standard `for ... range` loop has a critical architectural vulnerability:

```go
for val := range myChan {
    // If upstream hangs or never closes myChan,
    // this loop blocks permanently and leaks the goroutine!
}
```

The standard `for ... range` syntax cannot listen to `<-ctx.Done()`. To make channel consumption cancellation-safe, developers are forced to write verbose nested `select` blocks at every consumption point:

```go
for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case val, ok := <-myChan:
        if !ok {
            return nil
        }
        // Process val...
    }
}
```

Repeating this boilerplate across multiple packages leads to code bloat and increases the risk of omitting cancellation checks.

---

## Architectural Mechanics

The `OrDone` pattern wraps any incoming receive-only channel with a background proxy goroutine that continuously multiplexes stream intake against `ctx.Done()`:

```text
                    ┌──► ctx.Done() ──► [ Closes 'output' & exits ]
 [ input <-chan T ]─┤
                    └──► item ───────► [ output <- item ]
                                         │
                                         ▼
                            for val := range output { ... }
```

The resulting channel safely closes whenever upstream closes **or** when the context cancels.

---

## Core Invariants

1. **Guaranteed Channel Teardown:** The worker goroutine owns the produced channel and guarantees closure via `defer close(output)`.
2. **Egress Protection:** Writing to `output` is wrapped in a nested `select` with `<-ctx.Done()`. If downstream stops reading midway through, the routine unblocks and exits rather than getting trapped on an unbuffered send.
3. **Backpressure Preservation:** `output` is unbuffered, maintaining zero-allocation $O(1)$ memory constraints between producers and consumers.

---

## Public API

### `OrDone[T any](ctx context.Context, input <-chan T) <-chan T`
Wraps a readable channel with context cancellation awareness.

* **Parameters:**
  * `ctx`: Lifecycle context.
  * `input`: The raw upstream channel to monitor.
* **Returns:**
  * `<-chan T`: A sanitized channel that drains normally or closes immediately on context cancellation.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    ordonechannel "go-backend-patterns/concurrency/or_done_channel"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    rawStream := make(chan int)

    // Upstream producer
    go func() {
        defer close(rawStream)
        for i := 1; ; i++ {
            rawStream <- i
            time.Sleep(100 * time.Millisecond)
        }
    }()

    // Safe consumption with standard range
    for val := range ordonechannel.OrDone(ctx, rawStream) {
        fmt.Println("Received:", val)
    }

    fmt.Println("Cleanly exited loop without goroutine leaks")
}
```

---

## Verification

Execute the test suite with race detection and leak verification:

```powershell
go test -v -race ./concurrency/or_done_channel/...
```