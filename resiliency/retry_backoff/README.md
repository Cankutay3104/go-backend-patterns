# resiliency/retry_backoff

A context-aware, allocation-conscious retry mechanism implementing Exponential Backoff with Full Jitter to mitigate thundering herds.

---

## The Problem

When a downstream dependency experiences transient degradation, uncoordinated client retries generate synchronized waves of traffic. Deterministic intervals cause thousands of clients to retry in lockstep, knocking recovering services back into failure:

```text
T = 0ms     [Downstream Outage Occurs]
T = 100ms   10,000 Clients Retry Simultaneously ---> Total Dependency Collapse
T = 300ms   10,000 Clients Retry Simultaneously ---> Dependency Stalls Permanently
```

---

## Mathematical Model: Full Jitter

To desynchronize incoming retry waves, each backoff duration is drawn uniformly at random from an exponentially scaling window:

```text
T_cap   = min(MaxBackoff, BaseBackoff * (2 ^ attempt))
T_sleep = UniformRandom(0, T_cap)
```

This ensures retry traffic is dispersed smoothly across time rather than concentrated at discrete intervals.

---

## Architectural Mechanics

The retry loop executes an arbitrary fallible operation within the lifecycle of a parent `context.Context`:

```text
                     +-------------------+
                     |   Execute fn()    |
                     +---------+---------+
                               |
                        err == nil?
                        /         \
                  YES  /           \  NO
                      v             v
                [Return nil]   Is Retryable(err)?
                               /            \
                         NO   /              \  YES
                             v                v
                       [Return err]    Attempt < MaxAttempts?
                                        /                 \
                                  NO   /                   \  YES
                                      v                     v
                                [Return err]       Compute Full Jitter
                                                            |
                                                            v
                                                  select {
                                                  case <-ctx.Done(): return ctx.Err()
                                                  case <-timer.C:    loop to next attempt
                                                  }
```

---

### Core Invariants

1. **Context Responsiveness:** Sleeps yield immediately if `ctx.Done()` fires, preventing abandoned goroutines from holding socket and memory pools.
2. **Deterministic PRNG Control:** The random number generator is decoupled via interface or custom seed to enable 100% reproducible unit tests.
3. **Selective Retryability:** Non-transient errors fail fast without consuming retry budgets.
4. **Timer Leak Prevention:** Timers are explicitly stopped and drained rather than orphaned in the Go runtime.

---

## Public API

### `type Config struct`
Defines execution constraints:
- `MaxAttempts int`: Total execution attempts permitted (normalized to minimum 1).
- `BaseBackoff time.Duration`: Initial wait interval scaling factor.
- `MaxBackoff time.Duration`: Maximum duration ceiling for backoff intervals.

### `type RetryableFunc func(err error) bool`
Predicate signature evaluated on every non-nil error to determine whether the failure is transient and eligible for retry.

### `func AlwaysRetry(err error) bool`
Standard predicate helper that marks all non-nil errors as retryable.

### `func Execute(ctx context.Context, cfg Config, isRetryable RetryableFunc, fn func(ctx context.Context) error) error`
Executes `fn` up to `cfg.MaxAttempts` times using Exponential Backoff with Full Jitter. Returns `nil` on success, the terminal downstream error on exhaustion or non-retryable classification, or `ctx.Err()` upon context cancellation.