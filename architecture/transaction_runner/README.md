# architecture/transaction_runner

An infrastructure abstraction managing database transaction lifecycles, panic-safe rollbacks, and atomic execution without leaking driver handles into domain services.

---

## The Problem

Managing database transactions directly inside domain services introduces two architectural liabilities:

1. **Leaky Abstractions:** Passing `*sql.Tx` directly into domain methods couples business logic to relational database drivers, violating Clean Architecture.
2. **Resource Leaks on Unhandled Errors and Panics:** If business logic panics or returns prematurely without reaching `tx.Rollback()`, open row locks remain active and database connection pools become exhausted.

---

## Architectural Mechanics

The Transaction Runner manages the transaction boundary using higher-order closures and context propagation:

```text
[Domain Service] ──► runner.InTransaction(ctx, func(txCtx) error { ... })
                              │
                              ▼
                       BeginTx(ctx)
                              │
                       Execute fn(txCtx)
                        ├── Error / Panic ──► Rollback (Connection returned safely)
                        └── Success (nil)  ──► Commit (Changes persisted)
```

### Core Invariants

1. **Guaranteed Cleanup:** If the supplied closure returns an error or triggers a runtime panic, the runner executes a rollback before returning or re-panicking.
2. **Clean Domain Signatures:** Domain repositories accept standard `context.Context` without requiring explicit transaction pointers in method parameters.
3. **Context-Aware Resolution:** Repositories extract transaction handles transparently from the context when present, or execute against the base pool otherwise.
4. **Leak-Free Concurrency:** Verified using `internal/leaktest` and `-race` across concurrent worker transactions.

---

## Public API

### `type TxRunner interface`
Defines the atomic execution contract:
`InTransaction(ctx context.Context, fn func(ctx context.Context) error) error`

### `func New(db DB) *Runner`
Constructs a transaction runner wrapping a database interface supporting `BeginTx`.

### Context Helpers
- `WithTx(ctx context.Context, tx Tx) context.Context`: Injects a transaction handle into the context.
- `FromContext(ctx context.Context) (Tx, bool)`: Extracts an active transaction handle if present.
