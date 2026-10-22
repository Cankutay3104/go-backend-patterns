# memory_performance/sync_pool_buffer

A bounded, allocation-conscious bytes.Buffer pool with capacity ceilings to prevent memory leaks and GC latency spikes.

---

## The Problem

High-throughput backend services allocating dynamic buffers in hot paths suffer from severe Garbage Collector (GC) pressure. While `sync.Pool` eliminates recurring heap allocations by recycling objects, an unbounded buffer pool introduces the **bloated buffer memory leak**:

When an occasional large request (e.g., a 32MB CSV export or large JSON batch) borrows a buffer, Go's runtime expands the backing array:

```text
Underlying Slice: cap = 32MB, len = 32MB
```

Upon completion, calling `buf.Reset()` only sets `len = 0`; the 32MB backing array remains allocated. Returning this buffer to a raw `sync.Pool` traps multi-megabyte allocations in memory permanently. Subsequent tiny 500-byte requests reuse these giant buffers, inflating resident set size (RSS) memory indefinitely.

---

## Architectural Mechanics

This package wraps `sync.Pool` with bounded capacity controls:

```text
[Caller: Get()]
       │
       ▼
Pool.pool.Get() ──► nil? ──► Allocate new bytes.Buffer with initCap
       │
       ▼
Buffer Issued (Clean, len = 0)
       │
       ▼
[Caller: Put(buf)]
       │
       ▼
buf.Cap() > maxCap ?
       ├── YES ──► Discard (Allow GC to reclaim outlier allocation)
       └── NO  ──► buf.Reset() ──► Pool.pool.Put(buf)
```

### Core Invariants

1. **Capacity Clamping:** Buffers exceeding `maxCap` are never returned to the pool, preventing memory bloat from outlier payloads.
2. **Zero Information Leakage:** Every buffer returned via `Put()` is reset (`len = 0`) before re-entering the pool, preventing cross-tenant data leaks.
3. **Optimized Baseline Allocation:** Buffers initialized by the pool allocate an initial capacity (`initCap`) to prevent slice resizing reallocations on typical payloads.
4. **Leak-Free Concurrency:** Verified with `internal/leaktest` and `-race` under heavy goroutine contention.

---

## Public API

### `func New(initCap int, maxCap int) *Pool`
Constructs a bounded buffer pool. Normalizes non-positive parameters to safe production defaults.

### `func (p *Pool) Get() *bytes.Buffer`
Retrieves a clean, reset `*bytes.Buffer` from the pool (or allocates a new one if the pool is empty).

### `func (p *Pool) Put(buf *bytes.Buffer)`
Returns a buffer to the pool after verifying its capacity does not exceed `maxCap`. Drops oversized buffers and handles `nil` pointers gracefully.
