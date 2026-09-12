# concurrency/fanout_fanin

A production-grade implementation of the Fan-Out / Fan-In concurrency pattern in Go, utilizing generics, unbuffered channel multiplexing, and deterministic leak-free teardown via `context.Context` and `sync.WaitGroup`.

---

## The Problem

A standard sequential pipeline routes stream elements through a chain of single-goroutine stages:

```text
[ Source ] ──chan──► [ Expensive Stage (100ms) ] ──chan──► [ Sink ]
```

When a processing stage performs heavy computational work (e.g., cryptographic hashing, image transformation) or high-latency I/O operations (e.g., external microservice calls), a single worker becomes the bottleneck of the entire pipeline. Upstream stages halt due to channel backpressure, leaving available multi-core CPU resources underutilized.

---

## Architectural Mechanics

The Fan-Out / Fan-In pattern divides work across parallel routines and aggregates the outputs back into a single unified stream:

```text
                                ┌──► [ Worker 1 ] ──┐
                                │                   │
[ Upstream Channel ] ── Fan-Out ┼──► [ Worker 2 ] ──┼── Fan-In ──► [ Merged Channel ]
                                │                   │
                                └──► [ Worker N ] ──┘
```

1. **Fan-Out (Work Distribution):** Multiple worker goroutines read from the **same** upstream input channel. Go's runtime scheduler guarantees each item is delivered to exactly one worker without data races or duplicate processing.
2. **Fan-In (Multiplexing):** The independent output streams produced by each worker are consolidated into a single destination channel via `Merge`, allowing downstream consumers to read from a single pipe.

---

## Core Invariants

1. **Deterministic Channel Ownership:** Each worker goroutine creates and owns its individual output channel, closing it cleanly upon termination via `defer close(out)`.
2. **Coordinated Multiplex Closure:** The merged output channel is owned by `Merge`. It uses a `sync.WaitGroup` to track all active input readers. A dedicated cleaner goroutine waits for `wg.Wait()` before invoking `close(out)`, preventing runtime panics from sending to a closed channel.
3. **Guarded Ingress and Egress:** Every channel receive and send operation is wrapped inside a `select` block multiplexed with `case <-ctx.Done():`. This ensures that if downstream cancels early, neither workers nor the merger routine block permanently on unbuffered writes.
4. **Order Non-Determinism:** Because worker goroutines execute concurrently across available CPU cores, output arrival order is non-deterministic. Consumers must not rely on strict FIFO sequence when using Fan-Out.

---

## Public API

### `Merge[T any](ctx context.Context, channels ...<-chan T) <-chan T`
Multiplexes an arbitrary number of inbound channels into a single unified receive-only channel.

* Spawns an intake goroutine per input channel.
* Safely terminates when all source channels close or when `ctx` signals cancellation.

### `FanOut[I, O any](ctx context.Context, in <-chan I, worker int, transform func(I) O) <-chan O`
Distributes computation across `worker` background goroutines.

* Defensively normalizes `worker <= 0` to `1`.
* Instantiates `worker` independent processing routines reading from `in`.
* Automatically passes worker outputs to `Merge` and returns the consolidated channel.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    fanoutfanin "go-backend-patterns/concurrency/fanout_fanin"
    "go-backend-patterns/concurrency/pipeline"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // 1. Ingest numbers into an initial streaming channel
    source := pipeline.Generate(ctx, 1, 2, 3, 4, 5, 6, 7, 8)

    // 2. Distribute heavy computation across 4 concurrent workers
    expensiveTask := func(n int) string {
        time.Sleep(50 * time.Millisecond) // Simulated CPU/IO latency
        return fmt.Sprintf("Processed #%d by worker", n)
    }

    results := fanoutfanin.FanOut(ctx, source, 4, expensiveTask)

    // 3. Consume the multiplexed output stream
    for item := range results {
        fmt.Println(item)
    }
}
```

---

## Verification

Run the test suite under the Go race detector and leak detection harness:

```powershell
go test -v -race ./concurrency/fanout_fanin/...
``` z