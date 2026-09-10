package pipeline

import "context"

// Converts a discrete sequence of values into a receive-only channel.
// It strems items sequentially in the background and closes the channel on completion or cancellation.
func Generate[T any](ctx context.Context, values ...T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)

		for _, value := range values {
			// Ensures we do not block forever if the consumer/receiver/caller stops reading.
			select {
			// Abort immediately if downstream cancels or times out.
			case <-ctx.Done():
				return
			// Send the current value when the consumer is ready to receive.
			case out <- value:
			}
		}
	}()

	return out
}

// I = Input type, O = Output Type
// Map applies a transformation function to each element streamed from the input channel.
// It returns a new receive-only channel carrying the transformed results.
func Map[I, O any](ctx context.Context, in <-chan I, transform func(I) O) <-chan O {
	out := make(chan O)

	go func() {
		defer close(out)

		for {
			// Wait for a cancellation signal or an input item.
			select {
			case <-ctx.Done():
				return

			case item, ok := <-in:
				if !ok {
					// If upstream (where data originates) closed the channel, all work is complete.
					return
				}

				// Apply the transformation synchronously (we don't create another goroutine for it).
				result := transform(item)

				// Deliver the transformed data.
				// This separate select ensures we never freeze if downstream (where data goes) abandons the stream.
				select {
				case <-ctx.Done():
					return
				case out <- result:
				}
			}
		}
	}()

	return out
}

// Filter evaluates each element from the input channel using a predicate function.
// It forwards only elements matching the condition to the returned channel.
func Filter[T any](ctx context.Context, in <-chan T, predicate func(T) bool) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return

			case item, ok := <-in:
				if !ok {
					return
				}

				// Skip items that do not meet the criteria.
				if !predicate(item) {
					continue
				}

				// Preventing stranded goroutines.
				select {
				case <-ctx.Done():
					return
				case out <- item:
				}
			}
		}
	}()

	return out
}
