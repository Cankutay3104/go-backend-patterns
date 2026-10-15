package httpenvelope_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	httpenvelope "go-backend-patterns/networking/http_envelope"
)

// Verifies transport headers, status code propagation, null error serialization, and data payload integrity on success responses.
func TestWriteJSON_Success(t *testing.T) {
	rec := httptest.NewRecorder()

	status := http.StatusOK
	type sampleData struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	payload := sampleData{ID: 1, Name: "sample"}

	// We invoke WriteJSON; therefore, it should serialize the payload cleanly without network or encoding errors.
	err := httpenvelope.WriteJSON(rec, status, payload)
	if err != nil {
		t.Fatalf("error from WriteJSON: expected nil, got: %v", err)
	}

	// Verifying that the HTTP status code matches the input.
	if rec.Code != status {
		t.Fatalf("status code mismatch: expected %d, got %d", status, rec.Code)
	}

	// For that purpose, we need to check the MIME type header is set so that HTTP clients parse JSON correctly.
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type mismatch, got: %v", rec.Header().Get("Content-Type"))
	}

	// We unmarsahl the serialized response bytes to verify our Envelope contract.
	var res httpenvelope.Envelope
	err = json.Unmarshal(rec.Body.Bytes(), &res)
	if err != nil {
		t.Fatalf("Unable to deserialize response body: %v", err)
	}

	// Invariants check: status must be 'success', code must match, and the error pointer must remain nil. That's why we validate each field individually.
	if res.Status != "success" {
		t.Fatalf("expected status %q, got %q", "success", res.Status)
	}
	if res.Code != status {
		t.Fatalf("expected envelope code %d, got %d", status, res.Code)
	}
	if res.Error != nil {
		t.Fatalf("expected error field to be nil, got: %+v", res.Error)
	}

	// When Go unmarshals JSON into 'any', numbers decode as float64 and objects decode as map[string]any. Therefore, we do a type assertion on Data to verify payload integrity.
	dataMap, ok := res.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected res.Data to be map[string]any, got: %T", res.Data)
	}

	if dataMap["name"] != "sample" || dataMap["id"] != float64(1) {
		t.Fatalf("unexpected data content: expected id=1 and name='sample', got id=%d and name=%v", dataMap["id"], dataMap["name"])
	}
}

// Verifies status code propagation, Content-Type headers, null data serialization, and structured error payload integrity.
func TestWriteError_Success(t *testing.T) {
	rec := httptest.NewRecorder()
	status := http.StatusNotFound
	expectedMsg := "resource not found"
	expectedDetails := "user ID -12 does not exist."

	// We trigger WriteEror to ensure failure envelopes serialize without dropping context.
	httpenvelope.WriteError(rec, status, expectedMsg, expectedDetails)

	if rec.Code != status {
		t.Fatalf("status code mismatch: expected %d, got %d", status, rec.Code)
	}

	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("content type mismatch, got: %v", rec.Header().Get("Content-Type"))
	}

	// Unmarshaling the serialized byte stream to verify Envelope contract again.
	var res httpenvelope.Envelope
	err := json.Unmarshal(rec.Body.Bytes(), &res)
	if err != nil {
		t.Fatalf("error during deserialization: got %v", err)
	}

	// Invariants check
	if res.Status != "error" {
		t.Fatalf("expected status 'error', got %v", res.Status)
	}

	if res.Code != http.StatusNotFound {
		t.Fatalf("expected status code %d, got %d", http.StatusNotFound, res.Code)
	}

	// In an error envelope, data payload must explicitly serialize to JSON null, therefore; res.Data must be nil.
	if res.Data != nil {
		t.Fatalf("expected data %v, got %v", nil, res.Data)
	}

	// Error object must not be nil, that's why we guard against a nil pointer deference first.
	if res.Error == nil {
		t.Fatalf("res.Error cannot be nil.")
	}

	// Verifying the context of ErrorDetail fields.
	if res.Error.Message != expectedMsg {
		t.Fatalf("error message mismatch: expected 'resource not found', got %v", res.Error.Message)
	}

	if res.Error.Details != expectedDetails {
		t.Fatalf("error details mismatch: expected 'user ID -12 does not exist.', got %v", res.Error.Details)
	}
}

func TestWriteJSON_SerializationFailure(t *testing.T) {
	rec := httptest.NewRecorder()

	// In Go, channels cannot be serialized into JSON. That's why we pass one to trigger json.UnsupportedTypeError.
	unsupportedPayload := make(chan int)

	err := httpenvelope.WriteJSON(rec, http.StatusOK, unsupportedPayload)
	if err == nil {
		t.Fatalf("expected serialization error for unsupported type, got nil")
	}

	// For that purpose, we verify that nothing was written to the stream when marshaling failed.
	if rec.Body.Len() != 0 {
		t.Fatalf("expected 0 bytes written too body on error, got %d bytes: %q", rec.Body.Len(), rec.Body.String())
	}
}
