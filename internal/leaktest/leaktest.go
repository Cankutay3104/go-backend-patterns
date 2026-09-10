package leaktest

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// captureActiveGoroutines inspects the global runtime stack dump and filters out infrastructure frames originating from the testing harness and runtime daemon threads.
func activeGoroutines() []string {
	stackBuffer := make([]byte, 2<<20) // allocating 2 MB diagnostic buffer
	bytesWritten := runtime.Stack(stackBuffer, true)
	rawDump := string(stackBuffer[:bytesWritten])

	var filteredGoroutines []string
	for rawBlock := range strings.SplitSeq(rawDump, "\n\n") {
		processedBlock := strings.TrimSpace(rawBlock)
		if processedBlock == "" {
			continue
		}

		// Disregard known runtime monitors and testing framework
		if strings.Contains(processedBlock, "testing.(*T).Run") || strings.Contains(processedBlock, "testing.RunTests") || strings.Contains(processedBlock, "leaktest.activeGoroutines") || strings.Contains(processedBlock, "signal.signal_recv") || strings.Contains(processedBlock, "runtime.goexit") {
			continue
		}

		filteredGoroutines = append(filteredGoroutines, processedBlock)
	}

	return filteredGoroutines
}

// containsStackTrace checks whether a specific stack signature is present within the baseline set.
func containsStackTrace(baselineSet []string, candidateTrace string) bool {
	for _, baselineTrace := range baselineSet {
		if baselineTrace == candidateTrace {
			return true
		}
	}
	return false
}

// Check captures the active goroutines at invocation and returns an evaluation closure.
// When the closure executes, it asserts that no rogue goroutines remain running after a bounded settling interval. Intended usage: defer leaktest.Check(t)()
func Check(tb testing.TB) func() {
	initialGoroutines := activeGoroutines()

	return func() {
		// Provide the Go runtime scheduler a small grace period to allow exiting worker goroutines to finish their cleanup routines.
		evaluationDeadline := time.Now().Add(1 * time.Second)
		for time.Now().Before(evaluationDeadline) {
			currentGoroutines := activeGoroutines()
			if len(currentGoroutines) <= len(initialGoroutines) {
				return
			}
			runtime.Gosched()
			time.Sleep(10 * time.Millisecond)
		}

		// If extraneous goroutines persist beyond the threshold, isolate the newly introduced stack traces and fail the test execution.
		for _, stackTrace := range activeGoroutines() {
			if !containsStackTrace(initialGoroutines, stackTrace) {
				tb.Errorf("leaktest: uncollected goroutine detected post-test execution:\n%s", stackTrace)
			}
		}
	}
}
