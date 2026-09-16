package gracefulshutdown

import (
	"context"
	"net/http"
	"time"

	errgroup "golang.org/x/sync/errgroup"
)

// Run manages the execution and graceful shutdown lifecycle of an http.Server.
// It accepts incoming requests until the context is canceled, immediately after it coordinates an in-flight/active request drain within the allocated shutdown timeout window.
func Run(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration) error {
	// Ensure a non-positive timeout falls back to a production-safe duration.
	if shutdownTimeout <= 0 {
		shutdownTimeout = 5 * time.Second
	}

	// We derive an errgroup linked to the root context. For that purpose, any critical listener failure immediately signals the shutdown watcher.
	g, gCtx := errgroup.WithContext(ctx)

	// Routine 1: Ingress/Input listener.
	// We run srv.ListenAndServe() to accept incoming TCP traffic asynchronously.
	g.Go(func() error {
		err := srv.ListenAndServe()
		// http.ErrServerClosed indicates an intentional shutdown initiated via srv.Shutdown(). That's why we treat it as a clean exit by returning nil rather than an error.
		if err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Routine 2: Shutdown watcher.
	// It blocks waiting for the cancellation signal from gCtx (such as an OS SIGTERM).
	g.Go(func() error {
		<-gCtx.Done()

		// We construct an independent background context with a bounded deadline. Since gCtx is already canceled at this point we do not reuse it.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		// srv.Shutdown rejects new connections, closes idle keep-alives, and waits for active in-flight HTTP requests to complete.
		return srv.Shutdown(shutdownCtx)
	})

	// We await the termination of both routines, furthermore; g.Wait() returns the first non-nil error encountered during execution or teardown/cleanup.
	return g.Wait()
}
