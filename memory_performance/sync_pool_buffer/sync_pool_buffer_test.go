package syncpoolbuffer_test

import (
	"bytes"
	"sync"
	"testing"

	leaktest "go-backend-patterns/internal/leaktest"
	syncpoolbuffer "go-backend-patterns/memory_performance/sync_pool_buffer"
)

// Verifies that newly created buffers start with zero length and an underlying backing capacity meeting the configured initCap.
func TestPool_InitialAllocation(t *testing.T) {
	initCap := 512
	maxCap := 2048
	p := syncpoolbuffer.New(initCap, maxCap)

	buffer := p.Get()
	defer p.Put(buffer)

	if buffer.Len() != 0 {
		t.Fatalf("expected initial buffer length 0, got %d", buffer.Len())
	}
	if buffer.Cap() < initCap {
		t.Fatalf("expected buffer capacity >= %d, got %d", initCap, buffer.Cap())
	}
}

// Ensures buffers returned to the pool are wiped cleani preventing cross-request data leaks and dirty reads.
func TestPool_ResetOnPut(t *testing.T) {
	p := syncpoolbuffer.New(256, 1024)

	buffer := p.Get()
	_, _ = buffer.WriteString("confidential tenant session token")

	if buffer.Len() == 0 {
		t.Fatalf("expected buffer to contain written bytes")
	}

	// Returning buffer to pool; therefore, Put must invoke buffer.Reset() before storing it.
	p.Put(buffer)

	reclaimed := p.Get()
	defer p.Put(reclaimed)

	// Length must be reset to zero so that the callers cannot observe stale data.
	if reclaimed.Len() != 0 {
		t.Fatalf("expected reclaimed buffer length 0, got %d", reclaimed.Len())
	}
}

// Proves that buffers expanded past maxCap are discarded and never recycled, preventing the bloated buffer memory leak.
func TestPool_CapacityClamping_OutlierDrop(t *testing.T) {
	initCap := 128
	maxCap := 512
	p := syncpoolbuffer.New(initCap, maxCap)

	buffer := p.Get()

	// Writing an outlier payload exceeding maxCap
	// This forces Go's runtime to reallocate the backing slice past maxCap
	exceedingPayload := make([]byte, 1024)
	_, _ = buffer.Write(exceedingPayload)

	if buffer.Cap() <= maxCap {
		t.Fatalf("expected buffer capacity to grow past maxCap %d, got %d", maxCap, buffer.Cap())
	}

	// Put must inspect Cap() and drop this buffer without pooling it
	p.Put(buffer)

	// Borrowing again: because the oversized buffer was discarded, the pool must trigger its base function to allocate a fresh baseline buffer.
	nextBuffer := p.Get()
	defer p.Put(nextBuffer)

	// If the bloated buffer had been retained, nextBuffer.Cap() would be >= 1024. That's the reason why capacity <= maxCap confirms the outlier was dropped.
	if nextBuffer.Cap() > maxCap {
		t.Fatalf("bloated buffer was recycled: capacity %d exceeds maxCap %d", nextBuffer.Cap(), maxCap)
	}
}

// Ensures passing a nil pointer does not panic or corrupt pool state.
func TestPool_NilSafety(t *testing.T) {
	p := syncpoolbuffer.New(256, 1024)

	// Must return cleanly without panic
	p.Put(nil)

	buffer := p.Get()
	if buffer == nil {
		t.Fatalf("expected non-nil buffer from Get after nil Put")
	}
	p.Put(buffer)
}

// Verifies thread safety and clean goroutine lifecycles under heavy parallel contention using internal/leaktest and the Go race detector.
func TestPool_ConcurrencyRace(t *testing.T) {
	// Enforcing leak checks; therefore, any orphan goroutines fail the test suite immediately.
	defer leaktest.Check(t)()

	p := syncpoolbuffer.New(512, 4096)
	var wg sync.WaitGroup

	workerNum := 50
	iterations := 100

	for i := range workerNum {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for j := range iterations {
				buffer := p.Get()

				// Simulate dynamic workloads with different results
				if (workerID+j)%10 == 0 {
					_, _ = buffer.Write(bytes.Repeat([]byte("X"), 8192))
				} else {
					_, _ = buffer.Write([]byte("standard payload serialization"))
				}

				p.Put(buffer)
			}
		}(i)
	}

	// Block until all 50 workers finish their iterations so that the race detector monitors all memory access under true contention.
	wg.Wait()
}
