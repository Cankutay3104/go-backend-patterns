# networking/http_envelope

A standardized JSON response wrapper ensuring predictable API contracts and defensive serialization across all endpoints.

---

## The Problem

APIs that return heterogeneous JSON structures based on the endpoint or the success/failure state force clients into defensive, error-prone parsing:

```json
// Endpoint A (Success)
{ "id": 1 }

// Endpoint B (Success)
{ "data": { "token": "123" } }

// Endpoint C (Error)
{ "error": "not found" }
```

This lack of uniformity makes it impossible to write a single, global HTTP interceptor on the frontend.

---

## Architectural Mechanics

The `http_envelope` package enforces a strict response schema. Every HTTP response is serialized into an `Envelope` struct containing metadata, an optional generic data payload, and an optional structured error payload.

```json
// Standardized Success
{
  "status": "success",
  "code": 200,
  "data": { ... },
  "error": null
}

// Standardized Error
{
  "status": "error",
  "code": 404,
  "data": null,
  "error": {
    "message": "resource not found",
    "details": "user ID 999 does not exist"
  }
}
```

### Core Invariants

1. **Consistent Keys:** The top-level keys (`status`, `code`, `data`, `error`) are always present in the JSON response.
2. **Type Agnosticism:** The `data` field leverages Go's empty interface to accept any serializable type (structs, slices, maps, primitives).
3. **Defensive Writing:** Header modification and status code injection happen strictly before writing the JSON payload to prevent `http.ErrBodyNotAllowed` anomalies.

---

## Public API

### `type Envelope struct`
The root JSON structure encapsulating the response.

### `type ErrorDetail struct`
A structured payload for client-facing error messages and debugging context.

### `func WriteJSON(w http.ResponseWriter, status int, data any) error`
Constructs a success envelope, sets the `Content-Type` header, and streams the JSON to the client.

### `func WriteError(w http.ResponseWriter, status int, message string, details string)`
Constructs an error envelope and safely writes it to the client, defaulting to a 500 Internal Server Error if serialization fails.

