package tunedhttpclient_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	leaktest "go-backend-patterns/internal/leaktest"
	tunedhttpclient "go-backend-patterns/networking/tuned_http_client"
)

// Verifies that an empty Config{} correctly falls back to safe production defaults.
func TestNew_DefaultConfiguration(t *testing.T) {
	defer leaktest.Check(t)()

	client := tunedhttpclient.New(tunedhttpclient.Config{})

	defaults := tunedhttpclient.DefaultConfig()

	if client.Timeout != defaults.TotalTimeout {
		t.Fatalf("expected client timeout %v, got %v", defaults.TotalTimeout, client.Timeout)
	}

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatal("client transport is not *http.Transport")
	}

	if transport.MaxIdleConns != defaults.MaxIdleConnections {
		t.Errorf("expected MaxIdleConns %d, got %d", defaults.MaxIdleConnections, transport.MaxIdleConns)
	}

	if transport.MaxIdleConnsPerHost != defaults.MaxIdleConnectionsPerHost {
		t.Errorf("expected MaxIdleConnsPerHost %d, got %d", defaults.MaxIdleConnectionsPerHost, transport.MaxIdleConnsPerHost)
	}

	if transport.ResponseHeaderTimeout != defaults.ResponseHeaderTimeout {
		t.Errorf("expected ResponseHeaderTimeout %v, got %v", defaults.ResponseHeaderTimeout, transport.ResponseHeaderTimeout)
	}

	if transport.IdleConnTimeout != defaults.IdleConnectionTimeout {
		t.Errorf("expected IdleConnTimeout %v, got %v", defaults.IdleConnectionTimeout, transport.IdleConnTimeout)
	}
}

// Verifies that caller-provided timeouts override defaults.
func TestNew_RespectsCustomConfig(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := tunedhttpclient.Config{
		TotalTimeout:              3 * time.Second,
		MaxIdleConnections:        250,
		MaxIdleConnectionsPerHost: 50,
		ResponseHeaderTimeout:     2 * time.Second,
		IdleConnectionTimeout:     45 * time.Second,
		TLSHandshakeTimeout:       2 * time.Second,
		ConnectTimeout:            1 * time.Second,
	}

	client := tunedhttpclient.New(cfg)

	if client.Timeout != 3*time.Second {
		t.Errorf("expected client timeout 3s, got %v", client.Timeout)
	}

	transport := client.Transport.(*http.Transport)
	if transport.MaxIdleConns != 250 {
		t.Errorf("expected MaxIdleConns 250, got %d", transport.MaxIdleConns)
	}
	if transport.MaxIdleConnsPerHost != 50 {
		t.Errorf("expected MaxIdleConnsPerHost 50, got %d", transport.MaxIdleConnsPerHost)
	}
}

// Verifies that total client timeout aborts requests that run too long.
func TestNew_EnforcesTotalTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Hangs longer than client total timeout
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	}))
	defer server.Close()

	// Client configured with strict 20ms overall timeout
	client := tunedhttpclient.New(tunedhttpclient.Config{
		TotalTimeout: 20 * time.Millisecond,
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	_, err = client.Do(req)
	if err == nil {
		t.Fatal("expected request to timeout, got nil error")
	}

	// In Go, client timeouts wrap context.DeadlineExceeded
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "Client.Timeout exceeded") {
		t.Fatalf("expected timeout error, got: %v", err)
	}
}

// Verifies that transport aborts when TTFB exceeds ResponseHeaderTimeout.
func TestNew_EnforcesResponseHeaderTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Delay writing headers
		time.Sleep(80 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Set ResponseHeaderTimeout shorter than the server delay, but keep TotalTimeout large
	client := tunedhttpclient.New(tunedhttpclient.Config{
		ResponseHeaderTimeout: 20 * time.Millisecond,
		TotalTimeout:          1 * time.Second,
	})

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}

	_, err = client.Do(req)
	if err == nil {
		t.Fatal("expected response header timeout error, got nil")
	}

	if !strings.Contains(err.Error(), "timeout awaiting response headers") {
		t.Fatalf("expected 'timeout awaiting response headers', got: %v", err)
	}
}

// Verifies that DrainAndClose safely drains the stream and closes the body without errors.
func TestDrainAndClose_RecyclesConnection(t *testing.T) {
	defer leaktest.Check(t)()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("stream payload"))
	}))
	defer server.Close()

	client := tunedhttpclient.New(tunedhttpclient.Config{})

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// Drain and close should consume any remaining unread bytes and close without error
	if err := tunedhttpclient.DrainAndClose(resp.Body); err != nil {
		t.Fatalf("expected clean drain and close, got: %v", err)
	}

	// Passing nil body should be a clean no-op
	if err := tunedhttpclient.DrainAndClose(nil); err != nil {
		t.Fatalf("expected nil error for nil body, got: %v", err)
	}

	// Reading from closed body should fail
	var buf [16]byte
	_, err = resp.Body.Read(buf[:])
	if !errors.Is(err, io.ErrClosedPipe) && err != io.EOF && !strings.Contains(err.Error(), "closed") {
		t.Fatalf("expected body to be closed, got read error: %v", err)
	}
}
