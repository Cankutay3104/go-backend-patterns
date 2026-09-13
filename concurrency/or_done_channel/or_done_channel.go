package ordonechannel

import (
	"context"
)

// OrDone wraps an arbitrary readable channel with context awareness. It returns a sanitized channel that closes automatically when upstream closes OR when the context cancels. Therefore, downstream consumers can safely use 'for val := range OrDone(...)' without risking goroutine leaks.
// Basically it can be utilized as a safeguard to shorten other function implementations.
func OrDone[T any](ctx context.Context, input <-chan T) <-chan T {
	output := make(chan T)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-input:
				if !ok {
					return
				}

				select {
				case <-ctx.Done():
					return
				case output <- val:
				}
			}
		}
	}()

	return output
}
