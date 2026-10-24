# architecture/cache_aside

A production-grade Cache-Aside engine combining TTL expiration, fallback computation, and Singleflight request coalescing to eradicate the Cache Stampede (Thundering Herd) problem.

---

## The Problem

Standard cache-aside implementations suffer from **Cache Stampedes**:

1. Under high traffic, hot key expiration causes thousands of concurrent goroutines to miss the cache simultaneously.
2. All goroutines execute identical fallback queries against the primary database concurrently.
3. Connection pools become exhausted, CPU saturates, and database crashes cascade upstream.

---

## Architectural Mechanics

The cache-aside engine wraps cache lookups with singleflight flight-deduplication barriers:

```text
Client Request ──► Cache Hit? ──► Return Cached Data
                        │
                      (Miss)
                        ▼
             Singleflight Group (DoChan)
              ├── Leader: Computes DB Query ──► Sets Cache with TTL ──► Broadcasts
              └── Followers: Block on Channel ──► Receive Broadcasted Result
```

### Core Invariants

1. **Stampede Elimination:** Strictly one downstream computation executes per unique missing key, regardless of concurrent request volume.
2. **Type Safety via Generics:** Utilizes Go generics (`[T any]`) to provide compile-time type safety without runtime interface casting.
3. **Context Cancellation Propagation:** Followers respect parent context deadlines without abandoning the leader's cache-warming work.
4. **Explicit Invalidation:** Provides atomic invalidation to maintain consistency upon entity mutations.
5. **Leak-Free Concurrency:** Validated with `internal/leaktest` and `-race`.

---

## Public API

### `type Cache[T any] interface`
Defines low-level store interactions (`Get`, `Set`, `Delete`).

### `type Service[T any] struct`
The high-level cache-aside orchestrator managing singleflight deduplication.

### `func (s *Service[T]) GetOrCompute(ctx context.Context, key string, ttl time.Duration, compute func(ctx context.Context) (T, error)) (T, error)`
Attempts cache retrieval; upon cache miss, coordinates a single deduplicated execution of `compute` and populates the cache with TTL.

### `func (s *Service[T]) Invalidate(ctx context.Context, key string) error`
Evicts a key from the cache to preserve write-path consistency.
