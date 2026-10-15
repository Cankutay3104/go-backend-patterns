package httpenvelope

import (
	"encoding/json"
	"net/http"
)

// Envelope standardizes all outgoing HTTP responses across the API. Wraps payloads and diagnostic errors in a consistent JSON contract.
type Envelope struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
	// Data holds any arbitrary JSON serializable payload (structs, slices, primitives).
	Data any `json:"data"`
	// Error is defined as a pointer; therefore, setting it to nil serializes cleanly to JSON 'null' on successful responses instead of emitting an empty object.
	Error *ErrorDetail `json:"error"`
}

// Contains structured diagnostic information for client-facing errors.
type ErrorDetail struct {
	Message string `json:"message"`
	Details string `json:"details"`
}

// WriteJSON marshals and streams a successful Envelope to the client. It serializes the payload before modifying any HTTP headers to prevent partial writes.
func WriteJSON(w http.ResponseWriter, status int, data any) error {
	envelope := Envelope{
		Status: "success",
		Code:   status,
		Data:   data,
		Error:  nil,
	}

	// We serialize the payload into memory first; for that purpose, if json.Marshal encounters an unsupported type, we fail fast without leaving the TCP stream corrupted.
	serialized, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	// Writing directly to the network socket can fail if the client aborts early, that's the reason we return the resulting I/O error to the caller.
	_, err = w.Write(serialized)
	return err
}

// WriteError constructs and writes a failure Envelope to the client. If internal serialization fails, it falls back to a plain-text 500 Internal Server Error.
func WriteError(w http.ResponseWriter, status int, message string, details string) {
	errDetail := ErrorDetail{
		Message: message,
		Details: details,
	}

	envelope := Envelope{
		Status: "error",
		Code:   status,
		Data:   nil,
		Error:  &errDetail,
	}

	serialized, err := json.Marshal(envelope)
	if err != nil {
		// If serialization fails, the failure is an internal infrastructure defect, moreover; we return immediately to avoid writing superfluous headers. (superflous header refers to go runtime warning 'http: superfluous response.WriteHeader call')
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(serialized)
}
