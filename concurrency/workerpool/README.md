# concurrency/workerpool

A bounded worker pool implementation that limits concurrent execution, absorbs load spikes, and guarantees deterministic lifecycle management.

---

## The Problem

Creating an unconstrained goroutine per incoming request (`go process(job)`) causes severe memory consumption under high load. Spawning tens of thousands of goroutines at once exhausts memory stacks, strains the Go runtime scheduler, and overwhelms downstream dependencies such as database connections and network sockets.

A bounded worker pool restricts concurrency to a fixed number of long-lived worker goroutines backed by a buffered queue, transforming traffic surges into controlled queueing latency rather than out-of-memory crashes.

---

## Architectural Mechanics & Lifecycle

```text
[ Caller ] ──Submit(ctx, task)──► [ taskQueue (chan Task) ]
                                              │
                               ┌──────────────┼──────────────┐
                               ▼              ▼              ▼
                         [ Worker 1 ]   [ Worker 2 ]   [ Worker 3 ]
```

1. Fixed Worker Allocation: A designated count of background worker goroutines consume tasks concurrently from an internal buffered channel.

2. Channel Ownership Discipline: The pool struct alone creates, writes to, and closes the task channel. Workers only consume from it.

3. Graceful Teardown (Stop): Closes the input queue to reject new work, allows workers to drain remaining queued tasks, and blocks until all workers terminate cleanly.

4. Context-Aware Backpressure: Callers supply a context.Context to Submit. If the queue is saturated, the submission blocks safely without leaking goroutines if the caller context times out.

