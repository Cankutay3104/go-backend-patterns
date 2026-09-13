# concurrency/errgroup_pipeline

An error-aware, streaming pipeline stage built with Go generics and `golang.org/x/sync/errgroup`. It combines concurrent throughput with coordinated error propagation, backpressure, and fail-fast context cancellation.

---

## The Problem

Standard streaming pipelines and fan-out primitives process data without native error awareness:

```go
transform func(I) O
```

In backend systems, transformations are rarely infallible. They perform disk I/O, database lookups, or external network requests:

```go
transform func(ctx context.Context, item I) (O, error)
```

This introduces several synchronization challenges:
1. **Unmonitored Failures:** Output channels transport values (`O`), leaving no channel-safe mechanism to deliver errors without multiplexing mixed types or dynamic interface casting.
2. **Resource Waste (Lack of Fail-Fast):** If item 3 out of 100,000 encounters an unrecoverable database error, running the remaining 99,997 operations squanders CPU time, memory, and downstream API quota.
3. **Complex Goroutine Teardown:** Manually wiring cancellation channels, mutexes to preserve the first error, and `sync.WaitGroup` counters across multiple workers introduces significant boilerplate and risk of deadlocks.

---

## Architectural Mechanics

`ParallelMap` binds worker lifecycles to an `errgroup.Group` derived from the incoming context:

```text
                   ┌──► [ Worker 1: transform() ] ──┐
                   │                                │
[ in <-chan I ] ───┼──► [ Worker 2: transform() ] ──┼──► [ out <-chan O ]
                   │         │                      │
                   └──► [ Worker N: transform() ] ──┘
                             │
                      (Returns Error)
                             │
                             ▼
         [ errgroup cancels gCtx automatically ]
         [ Sibling workers abort through ingress/egress selects ]
         [ wait() returns first non-nil error to caller ]
```

---

## Core Invariants

1. **Deterministic Fail-Fast Cancellation:** When any worker function returns a non-nil error, the internal `errgroup.WithContext` immediately cancels `gCtx`. Sibling workers unblock via their ingress and egress `select` boundaries and terminate cleanly.
2. **First-Error Guarantee:** The closure returned by `ParallelMap` invokes `g.Wait()`, ensuring the caller captures the exact initial error that triggered pipeline termination.
3. **Backpressure via Zero Buffering:** The output channel `out` is unbuffered. Producers cannot run ahead of downstream consumers, enforcing constant ($O(1)$) memory consumption.
4. **Isolated Cleaner Lifecycle:** `close(out)` is decoupled from the main constructor execution. A background routine monitors `g.Wait()` to close the channel only after all workers are guaranteed dead, eliminating send-on-closed-channel panics.

---

## Public API

### `ParallelMap[I, O any](ctx context.Context, in <-chan I, workers int, transform func(ctx context.Context, item I) (O, error)) (<-chan O, func() error)`

Spawns `workers` concurrent routines to process items from `in`.

* **Parameters:**
  * `ctx`: Parent context for lifecycle management.
  * `in`: Inbound receive-only channel.
  * `workers`: Desired concurrency count (normalized to minimum 1).
  * `transform`: Fallible callback receiving the group-derived context.
* **Returns:**
  * `<-chan O`: Stream emitting successfully processed items.
  * `func() error`: A blocking handle to wait for pipeline completion and retrieve the first operational failure.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    errgrouppipeline "go-backend-patterns/concurrency/errgroup_pipeline"
    "go-backend-patterns/concurrency/pipeline"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    source := pipeline.Generate(ctx, 10, 20, 30, 40, 50)

    transform := func(workerCtx context.Context, n int) (string, error) {
        if n == 30 {
            return "", fmt.Errorf("invalid payload: %d", n)
        }
        return fmt.Sprintf("value: %d", n*2), nil
    }

    out, wait := errgrouppipeline.ParallelMap(ctx, source, 3, transform)

    // Drain the stream
    for res := range out {
        fmt.Println("Received:", res)
    }

    // Inspect pipeline execution status
    if err := wait(); err != nil {
        fmt.Println("Pipeline aborted with error:", err)
        return
    }

    fmt.Println("Pipeline completed successfully")
}
```

---

## Verification

Execute the test suite with race detection and runtime goroutine leak analysis enabled:

```powershell
go test -v -race ./concurrency/errgroup_pipeline/...
```