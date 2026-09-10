# go-backend-patterns

Production-grade concurrency, networking, and system patterns written in idiomatic Go, using only the standard library.

This is not a snippet collection. Every pattern is an isolated, runnable package with a complete test suite, microbenchmarks, and commentary explaining *why* a mutex sits where it does, why a buffer is sized the way it is, and which failure mode the code defends against.

---

## Architectural Scope

This repository documents production patterns across five core areas of Go backend systems engineering:

* **Concurrency:** Coordinated execution, bounded worker pools, cancellation propagation, and leak-free channel lifecycles.
* **Networking:** Native `net/http` services, connection pool tuning, middleware composition, defensive deserialization, and streaming.
* **Resiliency:** Circuit breaking, token-bucket admission control, jittered backoff, and failure domain isolation.
* **Memory & Performance:** Zero-allocation patterns, `sync.Pool` reuse mechanics, cache-line false sharing prevention, and profiling.
* **Architecture:** Composable configurations, database transaction boundary management, and cache-aside integrity.

### Target Repository Blueprint
```text
go-backend-patterns/
├── internal/
│   └── leaktest/
├── concurrency/
│   ├── workerpool/
│   ├── pipeline/
│   ├── fanout_fanin/
│   ├── errgroup_pipeline/
│   └── or_done_channel/
├── networking/
│   ├── graceful_shutdown/
│   ├── tuned_http_client/
│   ├── middleware_chain/
│   ├── http_envelope/
│   └── http_streaming/
├── resiliency/
│   ├── circuit_breaker/
│   ├── token_bucket_limiter/
│   └── retry_backoff/
├── memory_performance/
│   ├── sync_pool_buffer/
│   └── zero_alloc_metrics/
└── architecture/
    ├── functional_options/
    ├── transaction_runner/
    └── cache_aside/
```

---

## Project Plan

### Internal Shared Harness (`internal/`)
Compiler-isolated utilities supporting test assertion suites across all packages.
* **`leaktest`:** Global runtime stack dump parser asserting zero stranded background goroutines post-execution.

### Concurrency Patterns (`concurrency/`)
Coordinated, bounded parallel execution primitives engineered with explicit lifecycle ownership.
* **`workerpool`:** Fixed-size worker pool preventing unbounded goroutine allocation and absorbing load surges.
* **`pipeline`:** Multi-stage channel processing avoiding intermediate slice materialization memory costs.
* **`fanout_fanin`:** Parallel replication of compute-heavy stages with multiplexed result aggregation.
* **`errgroup_pipeline`:** Coordinated multi-stage streams providing fast-fail cancellation and immediate short-circuiting.
* **`or_done_channel`:** Read cancellation encapsulation guarding infinite channel consumption loops against deadlocks.

### Networking Infrastructure (`networking/`)
High-throughput, clean lifecycle primitives built directly on `net/http`.
* **`graceful_shutdown`:** Zero-downtime server termination coordinating OS signal traps with in-flight request draining.
* **`tuned_http_client`:** Transport connection reuse, idle socket pools, and defensive timeout configurations.
* **`middleware_chain`:** Composable HTTP handler pipelines with zero-allocation context propagation.
* **`http_envelope`:** Standardized, secure JSON payload encoding defending against malformed payloads.
* **`http_streaming`:** Chunked transfer mechanics delivering continuous data streams under controlled memory limits.

### Resiliency Mechanics (`resiliency/`)
Defensive patterns isolating failure domains and avoiding cascade outages.
* **`circuit_breaker`:** Three-state state machine shedding traffic from degraded upstream dependencies.
* **`token_bucket_limiter`:** Lock-free admission control enforcing request burst thresholds.
* **`retry_backoff`:** Exponential retry policies utilizing full jitter to disperse thundering herds.

### Memory & Performance (`memory_performance/`)
Mechanical sympathy designs optimizing CPU cache lines and reducing garbage collection pressure.
* **`sync_pool_buffer`:** Reusable byte slice pools cutting allocation churn in high-load paths.
* **`zero_alloc_metrics`:** Contention-free, atomic diagnostic counters aligned to prevent false sharing.

### Structural Architecture (`architecture/`)
Decoupled enterprise software patterns providing maintainable domain boundaries.
* **`functional_options`:** Type-safe, self-documenting configuration design for extensible constructors.
* **`transaction_runner`:** Scoped database transaction boundaries ensuring deterministic commit and rollback semantics.
* **`cache_aside`:** Resilient dual-read pattern preserving database consistency during cache invalidations.

---

## Engineering Invariants

Every implementation in this repository adheres to five non-negotiable rules:

1. **Channel Ownership:** A channel has exactly one owner. The owner is the writer, and the owner is the only goroutine permitted to close it. No function ever closes a channel it did not create.
2. **Guarded Sends & Receives:** Selecting on `ctx.Done()` when receiving, but sending unguarded, is a cancellation leak. Both directions must be guarded against cancellation.
3. **`-race` is Not a Leak Detector:** The race detector verifies memory synchronization; it does not detect goroutines that never terminate. Every concurrency test pairs `-race` with runtime stack leak assertions.
4. **Injected Clocks:** Anything driven by durations—timeouts, rate refills, retry backoffs—accepts explicit timer primitives so test cases run deterministically without arbitrary sleeps.
5. **Errors Are Values:** Sentinel errors use `errors.Is`, typed errors use `errors.As`, and internal causes are wrapped using `%w` to preserve inspection.

---

## Getting Started

### Prerequisites
* Go 1.22+ (Zero external dependencies required)
* Git

### Installation
Clone the repository:
```bash
git clone https://github.com/Cankutay3104/go-backend-patterns.git
cd go-backend-patterns
```

### Running Verification Suites
Run all unit tests across the repository with race detection:
```bash
go test -v -race ./...
```

Run leak detection and assertions for an isolated package (e.g., workerpool):
```bash
go test -v -race ./concurrency/workerpool/...
```

Run microbenchmarks with memory allocation analysis:
```bash
go test -v -bench=. -benchmem ./...
```

---

## Author

**Cankutay Mutlu**
* GitHub: [@Cankutay3104](https://github.com/Cankutay3104)
* LinkedIn: [Cankutay Mutlu](https://linkedin.com/in/cankutay-mutlu-712460295/)

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.