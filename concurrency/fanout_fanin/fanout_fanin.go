package fanoutfanin

import (
	"context"
	"sync"
)

// Merge multiplexes multiple input channels into a single output channel (which is Fan-In). It coordinates concurrent readers and closes the merged output channel only after every input channel has been completely done processing.
func Merge[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	// 1. Invariant: The returned stream is unbuffered to enforce backpressure.
	out := make(chan T)

	// 2. Synchronization: Track active reader goroutines to ensure safe channel closure.
	var wg sync.WaitGroup

	for _, ch := range channels {
		wg.Add(1)

		go func(c <-chan T) {
			defer wg.Done()

			for {
				select { // We use an outer select on <-c to guard the ingress (incoming). If upstream stalls or never sends, ctx.Done() prevents us from freezing on the receive.
				case <-ctx.Done():
					return
				case val, ok := <-c:
					if !ok { // Shows that the operation is done. We check !ok to determine to whether terminate this reader if upstream closed this channel.
						return
					}
					select { // We use a nested select on out <- val to guard the egress (outgoing). The inner ctx.Done() guarantees that an unread send never traps the goroutine (avoid blocking permanently).
					case <-ctx.Done():
						return
					case out <- val:
					}
				}
			}
		}(ch)
	}

	// This is the cleaner routine that runs in the background to avoid blocking the caller.
	// ıt waits until all upstream readers invoke wg.Done() to safely close 'out' channel.
	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func FanOut[I, O any](ctx context.Context, in <-chan I, worker int, transform func(I) O) <-chan O {
	// 1. We make sure at least 1 worker runs.
	if worker <= 0 {
		worker = 1
	}

	// 2. We allocate a slice to hold the output channel of each worker.
	workerChannels := make([]<-chan O, worker)

	// 3. We spawn the workers, where each of them consumes from the shared 'in' channel.
	for i := 0; i < worker; i++ {
		out := make(chan O)
		workerChannels[i] = out

		go func() {
			defer close(out)

			for {
				// Ingress: We pull an item from the shared pipeline
				select {
				case <-ctx.Done():
					return
				case input, ok := <-in:
					if !ok {
						// If the shared channel is closed by upstream, work is done.
						return
					}

					// We execute the computational work synchronously on this worker thread.
					result := transform(input)

					// Egress: We deliver the result to this worker's output pipeline.
					select {
					case <-ctx.Done():
						return
					case out <- result:
					}
				}
			}

		}()
	}

	// 4. We pass all worker output channels into Merge for the Fan-In.
	return Merge(ctx, workerChannels...)
}
