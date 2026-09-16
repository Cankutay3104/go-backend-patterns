# networking/tuned_http_client

A production-grade, defensive wrapper around Go's `http.Client` and `http.Transport`, addressing connection starvation, socket descriptor leaks, and default transport bottlenecks.

---

## The Problem

Using Go's default HTTP client primitives in production introduces severe operational vulnerabilities:

1. **Unbounded Latency:** `http.DefaultClient` sets a timeout of `0` (infinite). If an upstream service stalls or silently drops packets without emitting a TCP FIN packet, the calling goroutine and socket remain open indefinitely, eventually causing file descriptor exhaustion.
2. **Idle Connection Starvation:** `http.DefaultTransport` caps `MaxIdleConnsPerHost` at `2`. Under high traffic, concurrent requests to the same host close connections rather than returning them to the pool, forcing subsequent requests to pay the full latency penalty of repeated TCP and TLS handshakes.
3. **Socket Descriptor Leaks:** If a caller neglects to drain `resp.Body` before closing it, the underlying transport resets the connection instead of recycling it into the idle keep-alive pool.

---

## Architectural Mechanics

A robust client establishes distinct timeout boundaries across each phase of the HTTP request lifecycle:

```text
[ Client.Timeout: Hard cap on total end-to-end round trip ]
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│ 1. DialContext: TCP Handshake (SYN -> SYN-ACK -> ACK)                 │
│    └─► Bounded by: net.Dialer.Timeout (default: 5s)                    │
│                                                                        │
│ 2. TLS Handshake: Certificate validation & key exchange                │
│    └─► Bounded by: http.Transport.TLSHandshakeTimeout (default: 5s)    │
│                                                                        │
│ 3. Send Request: Uploading request headers & payload                   │
│                                                                        │
│ 4. Wait for Response Headers: Time To First Byte (TTFB)                │
│    └─► Bounded by: http.Transport.ResponseHeaderTimeout (default: 10s) │
│                                                                        │
│ 5. Stream Body: Consuming data down the network wire                   │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Core Invariants

1. **Defensive Defaults:** If an empty `Config{}` is supplied, all timeout parameters and connection pool sizes default to production-safe values.
2. **Connection Pool Sizing:** `MaxIdleConnsPerHost` is scaled up to match `MaxIdleConns` (default: 100), eliminating socket thrashing under high concurrency.
3. **Safe Drain & Close Protocol:** `DrainAndClose` consumes unread response bytes into `io.Discard` before invoking `Close()`, ensuring the underlying TCP connection returns to the keep-alive pool cleanly.

---

## Public API

### `DefaultConfig() Config`
Returns standard production defaults:
* `ConnectTimeout: 5s`
* `TLSHandshakeTimeout: 5s`
* `ResponseHeaderTimeout: 10s`
* `IdleConnTimeout: 90s`
* `MaxIdleConns: 100`
* `MaxIdleConnsPerHost: 100`
* `TotalTimeout: 15s`

### `New(cfg Config) *http.Client`
Instantiates an `*http.Client` configured with the tuned transport and lifecycle timeouts.

### `DrainAndClose(body io.ReadCloser) error`
Consumes remaining bytes up to an 8KB buffer into `io.Discard` and closes the reader stream, preventing TCP socket resets.

---

## Usage

```go
package main

import (
    "context"
    "fmt"
    "net/http"

    tunedhttpclient "go-backend-patterns/networking/tuned_http_client"
)

func main() {
    client := tunedhttpclient.New(tunedhttpclient.DefaultConfig())

    req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://api.github.com", nil)
    if err != nil {
        panic(err)
    }

    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    // Guarantee clean connection recycling
    defer tunedhttpclient.DrainAndClose(resp.Body)

    fmt.Printf("Status: %s\n", resp.Status)
}
```

---

## Verification

```powershell
go test -v -race ./networking/tuned_http_client/...
```