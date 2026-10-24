# architecture/functional_options

A clean, backward-compatible constructor design pattern for Go backend services and clients without config struct bloat or telescoping constructors.

---

## The Problem

Go lacks function overloading and default parameter values. Configuring complex components leads to two antipatterns:

1. **Telescoping Constructors:** `NewServer`, `NewServerWithTimeout`, `NewServerWithTLS` cause exponential function sprawl.
2. **Naked Config Structs:** Passing `Config{}` creates zero-value ambiguities (distinguishing between uninitialized fields vs intentional zero values) and forces callers to supply empty structs.

---

## Architectural Mechanics

The Functional Options Pattern models configuration as closures that mutate a target struct during initialization:

```text
Default State ──► Option 1 (WithTimeout) ──► Option 2 (WithMaxConns) ──► Validate ──► *Server
```

### Core Invariants

1. **Sensible Defaults:** Calling the constructor with zero options produces a safe, fully functional instance.
2. **Backward Compatibility:** New configuration fields and option functions can be added without modifying existing constructor signatures.
3. **Defensive Validation:** Options validate inputs (rejecting negative durations, zero limits, or nil references) before applying them to internal state.
4. **Immutability After Construction:** Configuration options are executed strictly within `New()`; the component state cannot be modified unsafely once running.

---

## Public API

### `type Server struct`
The configured service instance encapsulating address, timeouts, connection limits, and logger implementations.

### `type Option func(*Server) error`
A functional closure that modifies a `*Server` and returns an error if the option argument is invalid.

### `func New(addr string, opts ...Option) (*Server, error)`
Initializes a Server with production defaults, applies provided options sequentially, validates the resulting configuration, and returns the ready-to-run instance.

### Standard Option Builders
- `WithTimeout(d time.Duration) Option`: Configures read/write timeout with positive duration validation.
- `WithMaxConns(n int) Option`: Sets concurrent connection ceiling (must be > 0).
- `WithTLS(tlsConf *tls.Config) Option`: Enables TLS encryption; rejects nil configurations.
- `WithLogger(logger Logger) Option`: Injects a custom logging interface.
