# resiliency/token_bucket_limiter

A zero-allocation, thread-safe Token Bucket rate limiter using lazy mathematical replenishment.

---

## The Problem

Unbounded incoming traffic causes CPU saturation, socket buffer exhaustion, and database connection pool starvation. Traditional rate limiters often spawn active background goroutines (`time.Ticker`) to refill counters periodically. Under thousands of concurrent tenants (e.g., rate limiting per user ID or IP), thousands of background tickers waste CPU cycles and trigger severe runtime scheduler thrashing.

---

## Architectural Mechanics

This implementation uses **lazy evaluation**: tokens are replenished mathematically only when a request arrives, requiring zero background goroutines:

```text
Incoming Request at time 'now'
              │
              ▼
    Acquire sync.Mutex
              │
              ▼
   Δt = now.Sub(lastRefill)
   tokens += Δt.Seconds() * rate
   tokens = min(capacity, tokens)
   lastRefill = now
              │
              ▼
       tokens >= n ?
       ├── YES ──► tokens -= n ──► Unlock ──► Return true  (Request Allowed)
       └── NO  ──────────────────► Unlock ──► Return false (HTTP 429 / Rejected)
```

### Core Invariants

1. **Zero Background Goroutines:** Replenishment is strictly a mathematical function of elapsed time ($\Delta t$).
2. **Fractional Precision:** Sub-millisecond intervals accumulate fractional tokens using `float64`, preventing truncation bias.
3. **Burst Tolerance:** Traffic bursts up to `capacity` are absorbed instantly, while sustained traffic is capped at `rate`.
4. **Thread Safety:** Mutex synchronization ensures atomic read-replenish-deduct cycles under `-race`.

---

## Public API

### `func New(rate float64, capacity float64) *Limiter`
Initializes a new limiter. The bucket starts full (`tokens = capacity`) to grant immediate burst capacity.

### `func (l *Limiter) Allow() bool`
Shorthand convenience helper equivalent to `AllowN(time.Now(), 1)`.

### `func (l *Limiter) AllowN(now time.Time, n float64) bool`
Executes the core lazy replenishment formula against timestamp `now`. If at least `n` tokens are available, it deducts `n` tokens atomically and returns `true`; otherwise, it returns `false` without modifying the token balance.