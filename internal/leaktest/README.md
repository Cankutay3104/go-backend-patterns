# internal/leaktest

Package `leaktest` provides runtime stack analysis to detect orphaned goroutines across concurrent test executions.

---

## The Problem

Go's built-in race detector (`go test -race`) inspects memory access synchronization. It identifies conflicting reads and writes across active threads, but remains blind to goroutines that never terminate.

A test suite can execute with zero data races while silently stranding goroutines on blocked channel operations or uncancelled loops, producing gradual memory leaks under production workloads.

---

## Architectural Mechanics

1. **Snapshot Baseline:** `defer leaktest.Check(t)()` captures the active stack frames before test execution begins.
2. **Graceful Settling:** On teardown, it polls `runtime.Stack` across a 1-second bounded window with `runtime.Gosched()` to allow asynchronous cleanups to complete.
3. **Differential Assertions:** Any persistent stack frame not present in the initial baseline or standard Go runtime infrastructure is reported as an uncollected goroutine leak.

---

## Usage

In any package test suite across this repository (concurrency, networking, resiliency, etc.):

```go
package mypackage_test

import (
    "testing"
    "go-backend-patterns/internal/leaktest"
)

func TestServiceExecution(t *testing.T) {
    // Captures active goroutines and verifies zero leaks upon test completion
    defer leaktest.Check(t)()

    // Run service workload, networking calls, or background tasks under test...
}
```