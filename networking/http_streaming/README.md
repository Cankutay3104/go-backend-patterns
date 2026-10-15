# networking/http_streaming

An event-streaming HTTP handler demonstrating Server-Sent Events (SSE), dynamic chunk flushing, and defensive client-disconnect handling.

---

## The Problem

Traditional HTTP endpoints buffer complete payloads in memory before writing headers and bodies. For continuous feeds, telemetry streams, or high-volume exports, this introduces high latency and unbounded memory allocation:

```text
Client ---> [Wait...] ---> [Wait...] ---> [Receive Entire 500MB JSON Payload]
```

If the client severs the connection halfway through, an unmonitored handler continues computing and allocating memory for an orphaned TCP socket.

---

## Architectural Mechanics

The `http_streaming` package leverages `http.Flusher` and `text/event-stream` to establish long-lived, unidirectional data pipelines.

```text
Client                          Server
  |                               |
  |--- GET /events -------------->|  Sets 'Content-Type: text/event-stream'
  |                               |  Sets 'Transfer-Encoding: chunked'
  |<-- event 1 (flushed) ---------|
  |<-- event 2 (flushed) ---------|
  |                               |
  x [Client Disconnects]          |
  |                               |  <- r.Context().Done() fires!
                                  |  Terminates processing loop cleanly
```

### Core Invariants

1. **Flusher Capability Verification:** The handler verifies that the underlying `http.ResponseWriter` implements `http.Flusher` before modifying headers.
2. **Deterministic Context Teardown:** Processing loops listen concurrently to an event source channel and `r.Context().Done()`. When a client disconnects, generation terminates immediately.
3. **SSE Protocol Compliance:** Every stream chunk follows the standardized `data: <payload>\n\n` wire format.

---

## Public API

### `type Event struct`
Represents an individual streamable chunk with an ID and data payload.

### `func StreamEvents(w http.ResponseWriter, r *http.Request, events <-chan Event) error`
Streams incoming events from a channel down the HTTP connection, flushing after every message and terminating when the channel closes or the request context cancels.