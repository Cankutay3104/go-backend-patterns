package httpstreaming

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrFlusherUnsupported indicates that the underlying transport cannot stream chunks.
var ErrFlusherUnsupported = errors.New("streaming unsupported: response writer does not implement http.Flusher")

// Event encapsulates a discrete Server-Sent Event message frame.
type Event struct {
	ID   string
	Data string
}

// StreamEvents reads events from a receive-only channel and flushes them chunk-by-chunk to the HTTP response stream. It halts immediately when the channel closes or when the client severs the connection.
func StreamEvents(w http.ResponseWriter, r *http.Request, events <-chan Event) error {
	// Probing interface capability; therefore, if the underlying transport lacks flushing capability, we fail fast before mutating headers.
	flusher, ok := w.(http.Flusher)
	if !ok {
		// The underlying transport cannot flush chunks immediately.
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return ErrFlusherUnsupported
	}

	// Configuring standard Server-Sent Events headers.
	// That's the reason intermediaries and browsers recognize this response as an open data stream.

	// Informs the browser/client to parse incoming data using the Server-Sent Events protocol.
	w.Header().Set("Content-Type", "text/event-stream")
	// Prevents reverse proxies (like Nginx or Cloudflare) from caching and delaying event chunks.
	w.Header().Set("Cache-Control", "no-cache")
	// Ensures the underlying TCP socket remains open indefinitely.
	w.Header().Set("Connection", "keep-alive")

	for {
		select {
		case <-r.Context().Done():
			// The client severed the connection, therefore; we terminate to avoid leaking goroutines.
			return r.Context().Err()

		case event, ok := <-events:
			if !ok {
				// Producer closed channel, stream ended normally.
				return nil
			}

			// Serializing the event into the standardized W3C SSE frame format.
			// Moreover, we check for write errors in case the socket broke mid-stream.
			_, err := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", event.ID, event.Data)
			if err != nil {
				return err
			}

			// Pushing buffered bytes down the wire immediately.
			flusher.Flush()
		}
	}
}
