# memory_performance/zero_alloc_metrics

A lock-free, zero-allocation metrics collector using sync/atomic primitives and static histogram buckets.

---

## The Problem

In high-throughput services (100k+ req/sec), telemetry instrumentation often incurs the **Observer Effect**:

1. Dynamic label formatting (`fmt.Sprintf`) triggers excessive heap allocations.
2. Shared metric registries guarded by `sync.RWMutex` cause CPU cache line bouncing and lock contention.
3. Garbage collector cycles triggered by metric tracking increase p99 tail latencies.

Observability must not cost more CPU cycles and memory bandwidth than the business logic itself.

---

## Architectural Mechanics

This implementation stores all counters, gauges, and latency buckets in a single, contiguous struct backed strictly by atomic primitives:

```text
[Hot Request Path] ──► atomic.AddUint64(&RequestsTotal, 1)  ──► 0 B/op, ~3ns
                   ──► ObserveLatency(time.Since(start))     ──► 0 B/op, ~5ns
                                                                   │
                                                                   ▼
                                                     [Static Bucket Array]
                                                     No heap allocation
                                                     No slice growth
                                                     No mutex locks
```

### Core Invariants

1. **Strict Zero-Allocation (0 B/op):** Incrementing counters, updating gauges, and observing latencies trigger zero heap escapes.
2. **Lock-Free Concurrency:** Uses hardware atomic operations (`sync/atomic`) to prevent thread contention.
3. **Fixed-Ceiling Histograms:** Predefined bucket boundaries eliminate dynamic slice reallocation and sorting overhead.
4. **Non-Blocking Snapshots:** Exporters take point-in-time value snapshots without blocking concurrent writers.
5. **Leak-Free Concurrency:** Verified with `internal/leaktest` and `-race`.

---

## Public API

### `func New() *Metrics`
Initializes a zero-allocation metrics collector with pre-configured static latency buckets.

### `func (m *Metrics) IncRequests()`
Atomically increments the total processed request counter.

### `func (m *Metrics) IncErrors()`
Atomically increments the total error counter.

### `func (m *Metrics) AddActive(delta int64)`
Atomically adjusts the current in-flight request gauge by delta (+1 on entry, -1 on exit).

### `func (m *Metrics) ObserveLatency(d time.Duration)`
Categorizes elapsed time into a fixed compile-time bucket using atomic increment, avoiding dynamic allocations.

### `func (m *Metrics) Snapshot() Snapshot`
Returns an immutable point-in-time value copy of all collected metrics without acquiring locks.
