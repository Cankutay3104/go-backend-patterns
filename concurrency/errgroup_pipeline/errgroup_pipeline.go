package errgrouppipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// ParallelMap distributes stream processing across N concurrent workers using an errgroup.Group.
// It applies a fallible transformation to each item and multiplexes successful results onto an output channel. Furthermore, if any worker encounters an error, the group automatically cancels the context across all workers.
func ParallelMap[I, O any](ctx context.Context, in <-chan I, workers int, transform func(ctx context.Context, item I) (O, error)) (<-chan O, func() error) {
	if workers <= 0 {
		workers = 1
	}

	// We derive an errgroup and a linked context from the parent context. Therefore, if any worker returns a non-nil error, gCtx cancels automatically for all workers.
	g, gCtx := errgroup.WithContext(ctx)

	out := make(chan O)

	// We spawn N workers managed directly by the errgroup rather than raw goroutines.
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for {
				// Ingress select: we guard the receive boundary against context cancellation. For that purpose, if upstream stalls or gCtx cancels, we return gCtx.Err() immediately.
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case input, ok := <-in:
					if !ok {
						return nil
					}

					result, err := transform(gCtx, input)
					if err != nil {
						return err
					}

					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case out <- result:
					}
				}
			}
		})
	}

	// Cleaner goroutine: runs in the background to avoid blocking the constructor.
	// We use _ = g.Wait() because we only need to know when all workers have finished so that we can safely close the out channel without race conditions.
	go func() {
		_ = g.Wait()
		close(out)
	}()

	// We encapsulate g.Wait() inside a closure so the caller can retrieve the first non-nil error.
	wait := func() error {
		return g.Wait()
	}

	return out, wait
}
