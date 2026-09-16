package gracefulshutdown_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	leaktest "go-backend-patterns/internal/leaktest"
	gracefulshutdown "go-backend-patterns/networking/graceful_shutdown"
)

// Verifies that in-flight requests complete cleanly when a shutdown signal arrives.
func TestRun_GracefulDrainInFlight(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Find an available free local port dynamically
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on free port: %v", err)
	}

	addr := listener.Addr().String()
	_ = listener.Close()

	requestStart := make(chan struct{})
	requestFinish := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		close(requestStart)
		// Simulate a long in-flight process
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("completed successfully"))
		close(requestFinish)
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- gracefulshutdown.Run(ctx, srv, 1*time.Second)
	}()

	// Wait briefly for server startup
	time.Sleep(30 * time.Millisecond)

	// Issue an in-flight request asynchronously
	clientErr := make(chan error, 1)
	var resBody string
	go func() {
		res, err := http.Get("http://" + addr + "/slow")
		if err != nil {
			clientErr <- err
			return
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		resBody = string(body)
		clientErr <- nil
	}()

	// Wait until the handler has accepted the request
	<-requestStart

	// Trigger shutdown while the request is actively sleeping in the handler
	cancel()

	// The client request must complete without error despite the shutdown
	if err := <-clientErr; err != nil {
		t.Fatalf("in-flight request failed during shutdown: %v", err)
	}

	if resBody != "completed successfully" {
		t.Fatalf("unexpected response body: %q", resBody)
	}

	// The server must terminate with nil error
	if err := <-serverErr; err != nil {
		t.Fatalf("expected clean server shutdown, got: %v", err)
	}
}

// Verifies that a hung handler times out when exceeding shutdownTimeout.
func TestRun_ShutdownTimeoutExceeded(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on free port: %v", err)
	}

	addr := listener.Addr().String()
	_ = listener.Close()

	requestStarted := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/hang", func(w http.ResponseWriter, r *http.Request) {
		close(requestStarted)
		// Hangs longer than the shutdown timeout
		time.Sleep(500 * time.Millisecond)
	})

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		// Enforce a strict 50ms shutdown timeout
		serverErr <- gracefulshutdown.Run(ctx, srv, 50*time.Millisecond)
	}()

	time.Sleep(30 * time.Millisecond)

	go func() {
		_, _ = http.Get("http://" + addr + "/hang")
	}()

	<-requestStarted
	cancel() // Trigger shutdown

	// Run must return context.DeadlineExceeded error because the handler hung past 50ms
	err = <-serverErr
	if err == nil {
		t.Fatal("expected deadline exceeded error, got nil")
	}

	// Give the hung handler goroutine a moment to finish so leaktest passes
	time.Sleep(500 * time.Millisecond)
}
