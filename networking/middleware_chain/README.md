# networking/middleware_chain

A zero-allocation, standard-library HTTP middleware chaining utility designed to compose HTTP handlers sequentially without deep nesting.

---

## The Problem

In Go, an HTTP middleware is typically defined as a function that accepts an `http.Handler` and returns an `http.Handler`. 

```go
type Middleware func(http.Handler) http.Handler
```

Applying multiple middlewares to a single route manually results in severe rightward drift and unmaintainable code structures:

```go
handler := middleware.Recover(
    middleware.Logger(
        middleware.RequireAuth(
            middleware.RateLimit(
                http.HandlerFunc(myBusinessLogic),
            ),
        ),
    ),
)
```

Therefore, injecting or removing a middleware in the middle of this stack is error-prone.

---

## Architectural Mechanics

The `middleware_chain` package provides a variadic composition function that iterates through a declared list of middlewares and wraps them around a target handler. 

```go
chain := middlewarechain.New(
    middleware.Recover,
    middleware.Logger,
    middleware.RequireAuth,
    middleware.RateLimit,
)

handler := chain.Wrap(http.HandlerFunc(myBusinessLogic))
```

### Core Invariants

1. **Standard Library Compatibility:** It strictly uses `net/http` interfaces. It does not introduce custom context structs or proprietary handler signatures.
2. **Execution Order Guarantee:** Middlewares execute in the exact order they are passed to the constructor. The first middleware in the list is the first to receive the incoming HTTP request.
3. **Immutability:** The chain instance is stateless. That is the reason you can define a core chain once (e.g., `baseChain`) and reuse it across multiple routes concurrently without race conditions.

---

## Public API

### `type Middleware func(http.Handler) http.Handler`
The standard signature for a function that wraps an HTTP handler.

### `func New(middlewares ...Middleware) Chain`
Instantiates a new middleware chain with the provided middlewares.

### `func (c Chain) Wrap(h http.Handler) http.Handler`
Applies the configured middlewares to the target handler.

### `func (c Chain) Append(middlewares ...Middleware) Chain`
Creates a new chain by appending additional middlewares to the existing chain, allowing for sub-routing (e.g., adding admin-only checks to a base API chain).

---

## Verification

Execute the test suite to verify execution order and context propagation:

```powershell
go test -v -race ./networking/middleware_chain/...
```
