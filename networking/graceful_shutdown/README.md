# networking/graceful_shutdown

Production-ready graceful shutdown runner for `http.Server` using `golang.org/x/sync/errgroup` and context cancellation.

---

## The Problem

When a server process terminates abruptly (such as from a container restart, deployment, or OS signal like `SIGTERM`), standard implementations sever active connections:

1. **Broken Connections:** In-flight HTTP requests drop mid-transfer (`ECONNRESET`).
2. **Partial State Changes:** Database transactions and file writes can be interrupted halfway through.
3. **Socket Thrashing:** The operating system forcefully resets connections instead of completing a clean TCP teardown handshake.

---

## Architectural Mechanics

The `Run` function coordinates two concurrent routines inside an `errgroup.Group`:

```text
[ OS Signal / Root Context Canceled ]
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
  [ Listener Routine ]    [ Shutdown Watcher Routine ]
    srv.ListenAndServe()    <-gCtx.Done() triggers
    Returns ErrServerClosed srv.Shutdown(timeoutCtx)
        │                   │
        └─────────┬─────────┘
                  ▼
         Clean Teardown (nil)
```

1. **Listener Routine:** Runs `srv.ListenAndServe()`. It accepts incoming traffic until `srv.Shutdown` triggers an intentional `http.ErrServerClosed`, which the runner treats as a clean exit (`nil`).
2. **Shutdown Watcher Routine:** Waits on `<-gCtx.Done()`. Once canceled, it starts `srv.Shutdown` with a dedicated bounded timeout context, rejecting new requests while letting active handlers complete.

---

## Core Invariants

1. **Defensive Timeout Default:** If `shutdownTimeout <= 0`, it defaults to `5s` so hung requests cannot block process termination indefinitely.
2. **Dedicated Shutdown Context:** The shutdown call uses an independent `context.WithTimeout(context.Background(), ...)` because the root context is already canceled by the time the watcher runs.
3. **Clean Error Mapping:** `http.ErrServerClosed` is filtered out so standard lifecycle termination returns `nil` instead of a false-positive error.

---

## Public API

### `Run(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration) error`

Executes the HTTP server and manages its graceful shutdown lifecycle.

* **`ctx`:** Root cancellation context (typically bound to OS signals via `signal.NotifyContext`).
* **`srv`:** Configured `*http.Server` instance.
* **`shutdownTimeout`:** Maximum time allowed for in-flight requests to complete before forcing a close.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "os/signal"
    "syscall"
    "time"

    gracefulshutdown "go-backend-patterns/networking/graceful_shutdown"
)

func main() {
    // Trap SIGINT and SIGTERM from the operating system
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    mux := http.NewServeMux()
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("ok"))
    })

    srv := &http.Server{
        Addr:         ":8080",
        Handler:      mux,
        ReadTimeout:  5 * time.Second,
        WriteTimeout: 10 * time.Second,
    }

    // Blocks until shutdown completes or fails
    if err := gracefulshutdown.Run(ctx, srv, 10*time.Second); err != nil {
        fmt.Printf("Server failure: %v\n", err)
    }
}
```

---

## Verification

```powershell
go test -v -race ./networking/graceful_shutdown/...
```