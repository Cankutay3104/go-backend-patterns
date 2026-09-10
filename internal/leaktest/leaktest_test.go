package leaktest

import (
	"sync"
	"testing"
	"time"
)

// mockTestingT intercepts error logging emitted by leaktest to verify detection assertions.
type mockTestingT struct {
	testing.TB
	failed   bool
	errorMsg string
}

func (m *mockTestingT) Errorf(format string, args ...any) {
	m.failed = true
	m.errorMsg = format
}

func TestCheck_NoLeak(t *testing.T) {
	defer Check(t)()

	var wg sync.WaitGroup

	// Simplified Go Routine creation with Wait Group
	wg.Go(func() {
		time.Sleep(10 * time.Millisecond)
	})

	// Block until the background worker has completely finished execution.
	wg.Wait()
}

func TestCheck_DetectLeakedGoroutine(t *testing.T) {
	mockTestContext := &mockTestingT{}

	// We isolate the leaking scenario inside an anonymous function closure.
	// This ensures that its deferred cleanup executes immediately when the closure returns, rather than waiting for the outer TestCheck_DetectLeakedGoroutine to finish.
	func() {
		// 1. Check() captures the baseline of all active goroutines before the workload runs.
		// It returns a teardown function (closure) containing the evaluation assertions.
		teardownAssertion := Check(mockTestContext)

		// 2. Schedule teardownAssertion() to run immediately upon exiting this closure scope.
		defer teardownAssertion()

		// 3. Intentionally induce a goroutine leak:
		// We allocate an unbuffered channel transmitting 'struct{}' (a zero byte empty struct for signaling without allocating memory).
		orphanChannel := make(chan struct{})

		// Spawn a background goroutine that attempts to send a zero byte value (struct{}{}).
		// Because orphanChannel is unbuffered and has no corresponding receiver, this send operation blocks indefinitely. Therefore, the goroutine hangs forever, retains its stack frame, and becomes a stranded leak.
		go func() {
			orphanChannel <- struct{}{}
		}()

	}()

	if !mockTestContext.failed {
		t.Fatalf("expected leaktest to register an uncollected goroutinee error, but it passed silently.")
	}
}
