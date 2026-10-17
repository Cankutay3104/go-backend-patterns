# resiliency/circuit_breaker

A thread-safe, zero-dependency Finite State Machine (FSM) Circuit Breaker designed to prevent cascading failures across distributed Go services.

---

## The Problem

When a downstream service degrades or hangs, calling services continue dispatching network calls. Goroutines block on unyielding I/O, saturating memory pools and thread limits until the host service collapses:

```text
Caller Service ---> [Goroutine Blocked] ---> [Socket Timeout Wait...] ---> Out of Memory (OOM)
Caller Service ---> [Goroutine Blocked] ---> [Socket Timeout Wait...]
Caller Service ---> [Goroutine Blocked] ---> [Socket Timeout Wait...]
```

Without fast-failing guards, partial degradation inevitably escalates into total system outages.

---

## Architectural Mechanics

The `circuit_breaker` package wraps arbitrary operations in a thread-safe execution harness governed by a three-state machine:

```text
                 +-----------------------+
                 |    Incoming Request   |
                 +-----------+-----------+
                             |
                   State == Open?
                   /             \
             YES  /               \  NO (Closed / Half-Open)
                 v                 v
     [Return ErrCircuitOpen]  [Execute Action()]
                               /               \
                         SUCCESS               FAILURE
                           /                       \
             [Reset / Transition Closed]     [Trip / Transition Open]
```

### Core Invariants

1. **Short-Circuit Protection:** When the breaker is `Open`, requests fail fast in memory without allocating network sockets or dispatching I/O calls.
2. **Defensive Concurrency Boundaries:** State transitions, failure counters, and timestamp invalidations are guarded by `sync.RWMutex`, ensuring `-race` clean execution under high concurrency.
3. **Controlled Probing:** In the `Half-Open` state, only authorized trial requests pass through to prevent overwhelming an already-recovering service.

---

## Public API

### `type State int`
Represents the current lifecycle state (`StateClosed`, `StateOpen`, `StateHalfOpen`).

### `type CircuitBreaker struct`
The synchronization primitive managing state transitions, failure counters, thresholds, and cooldown durations.

### `func New(threshold uint32, timeout time.Duration) *CircuitBreaker`
Initializes a new Circuit Breaker with a defined failure threshold and cooldown recovery window.

### `func (cb *CircuitBreaker) Execute(fn func() error) error`
Wraps a user-provided fallible operation, evaluating state prerequisites before execution and logging outcomes to drive state transitions.