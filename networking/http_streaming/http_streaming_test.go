package httpstreaming_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	httpstreaming "go-backend-patterns/networking/http_streaming"
)

// TestStreamEvents_Success verifies headers, payload serialization, and clean completion when the event channel closes normally.
func TestStreamEvents_Success(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)

	events := make(chan httpstreaming.Event)

	// Spawning an asynchronous producer for StreamEvents to consume concurrently without deadlocking.
	go func() {
		defer close(events)

		events <- httpstreaming.Event{ID: "1", Data: "hello"}
		events <- httpstreaming.Event{ID: "2", Data: "world"}
	}()

	err := httpstreaming.StreamEvents(rec, req, events)
	if err != nil {
		t.Fatalf("streaming error: expected nil, got %v", err)
	}

	// Verifying SSE protocol headers.
	if contType := rec.Header().Get("Content-Type"); contType != "text/event-stream" {
		t.Fatalf("content-type mismatch: expected text/event-stream, got %q", contType)
	}
	if cacheCont := rec.Header().Get("Cache-Control"); cacheCont != "no-cache" {
		t.Fatalf("cache-control mismatch: expected no-cache, got %q", cacheCont)
	}
	if conn := rec.Header().Get("Connection"); conn != "keep-alive" {
		t.Fatalf("connection mismatch: expected keep-alive, got %q", conn)
	}

	// Verifying that each chunk was serialized with strict W3C double-newline delimiters so that the consumer's parser can distinguish individual frames.
	expectedBody := "id: 1\ndata: hello\n\nid: 2\ndata: world\n\n"
	if rec.Body.String() != expectedBody {
		t.Fatalf("stream body mismatch: \nexpected: %q\ngot: %q", expectedBody, rec.Body.String())
	}
}

// TestStreamEvents_ClientDisconnect simulates a client severing the connection mid-stream.
func TestStreamEvents_ClientDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil).WithContext(ctx)

	events := make(chan httpstreaming.Event)

	go func() {
		events <- httpstreaming.Event{ID: "1", Data: "ping"}
		// The client aborts the connection mid-stream; therefore, we trigger cancel().
		cancel()
	}()

	err := httpstreaming.StreamEvents(rec, req, events)
	if err == nil {
		t.Fatalf("expected context.Canceled error, got nil")
	}

	// Verifying that the returned error wraps context.Canceled.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error mismatch: expected %v, got %v", context.Canceled.Error(), err)
	}
}

// nonFlushingResponseWriter satisfies http.ResponseWriter but intentionally omits Flush(). That's the reason we can simulate transports where http.Flusher is unsupported.
type nonFlushingResponseWriter struct {
	header     http.Header
	statusCode int
}

func (n *nonFlushingResponseWriter) Header() http.Header {
	return n.header
}

func (n *nonFlushingResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (n *nonFlushingResponseWriter) WriteHeader(statusCode int) {
	n.statusCode = statusCode
}

// TestStreamEvents_FlusherUnsupported verifies that passing a writer without Flush() fails fast with ErrFlusherUnsupported and sends a 500 Internal Server Error.
func TestStreamEvents_FlusherUnsupported(t *testing.T) {
	// We instantiate our non-flushing stub to make w.(http.Flusher) evaluate to false.
	w := &nonFlushingResponseWriter{
		header: make(http.Header),
	}

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	events := make(chan httpstreaming.Event)

	// We invoke StreamEvents which should fail immediately before reading any events.
	err := httpstreaming.StreamEvents(w, req, events)
	if err == nil {
		t.Fatalf("expected ErrFlusherUnsupported, got nil")
	}

	// Verifying that the returned error wraps or matches our defined sentinel error.
	if !errors.Is(err, httpstreaming.ErrFlusherUnsupported) {
		t.Fatalf("error mismatch: expected %v, got %v", httpstreaming.ErrFlusherUnsupported, err)
	}

	// Also, we verify that http.Error wrote an HTTP 500 status code to the client.
	if w.statusCode != http.StatusInternalServerError {
		t.Fatalf("status code mismatch: expected %d, got %d", http.StatusInternalServerError, w.statusCode)
	}
}
