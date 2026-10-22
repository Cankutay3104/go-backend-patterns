package zeroallocmetrics_test

import (
	"sync"
	"testing"
	"time"

	leaktest "go-backend-patterns/internal/leaktest"
	zeroallocmetrics "go-backend-patterns/memory_performance/zero_alloc_metrics"
)

// Verifies monotonic counter increments and fluctuating gauge tracking.
func TestMetrics_CountersAndGauges(t *testing.T) {
	m := zeroallocmetrics.New()

	m.IncRequests()
	m.IncRequests()
	m.IncErrors()

	// Gauges fluctuate up and down; therefore, we test both positive and negative deltas/differences.
	m.AddActive(5)
	m.AddActive(-2)

	snapshot := m.Snapshot()

	if snapshot.RequestsTotal != 2 {
		t.Fatalf("expected RequestsTotal 2, got %d", snapshot.RequestsTotal)
	}

	if snapshot.ErrorsTotal != 1 {
		t.Fatalf("expected ErrorsTotal 1, got %d", snapshot.ErrorsTotal)
	}
	if snapshot.ActiveRequests != 3 {
		t.Fatalf("expected ActiveRequests 3, got %d", snapshot.ActiveRequests)
	}
}

// Verifies exact bucket placement across all boundary thresholds, including the catch-all overflow bucket.
func TestMetrics_ObserveLatency_Buckets(t *testing.T) {
	m := zeroallocmetrics.New()

	testCases := []struct {
		name           string
		duration       time.Duration
		expectedBucket int
	}{
		{name: "Sub-5ms", duration: 2 * time.Millisecond, expectedBucket: 0},
		{name: "Exact 5ms boundary", duration: 5 * time.Millisecond, expectedBucket: 0},
		{name: "Exact 10ms boundary", duration: 10 * time.Millisecond, expectedBucket: 1},
		{name: "Exact 25ms boundary", duration: 25 * time.Millisecond, expectedBucket: 2},
		{name: "Exact 50ms boundary", duration: 50 * time.Millisecond, expectedBucket: 3},
		{name: "Exact 100ms boundary", duration: 100 * time.Millisecond, expectedBucket: 4},
		{name: "Exact 250ms boundary", duration: 250 * time.Millisecond, expectedBucket: 5},
		{name: "Exact 500ms boundary", duration: 500 * time.Millisecond, expectedBucket: 6},
		{name: "Over 500ms (Overflow)", duration: 501 * time.Millisecond, expectedBucket: 7},
		{name: "Multi-second (Overflow)", duration: 5 * time.Second, expectedBucket: 7},
	}

	for _, tc := range testCases {
		m.ObserveLatency(tc.duration)
	}

	snapshot := m.Snapshot()

	// Buckets 0 and 7 received 2 observations each, while buckets 1-6 received 1 each; that's why we can assert the exact expected distribution across the array.
	expectedCounts := [zeroallocmetrics.NumBuckets]uint64{2, 1, 1, 1, 1, 1, 1, 2}

	for i := range zeroallocmetrics.NumBuckets {
		if snapshot.LatencyBuckets[i] != expectedCounts[i] {
			t.Fatalf("bucket %d count mismatch: expected %d, got %d", i, expectedCounts[i], snapshot.LatencyBuckets[i])
		}
	}
}

// Verifies lock-free atomic correctness under high thread contention while monitoring runtime goroutines with internal/leaktest.
func TestMetrics_ConcurrencyRace(t *testing.T) {
	// Guaring against lingering goroutines, leaktest verifies all workers exit cleanly before the test concludes.
	defer leaktest.Check(t)()

	m := zeroallocmetrics.New()
	var wg sync.WaitGroup

	numWorkers := 50
	iterations := 1000

	for range numWorkers {
		wg.Add(1)

		wg.Go(func() {
			defer wg.Done()

			for i := range iterations {
				m.IncRequests()

				// In-flight gauge increments on start and decrements on exit; that's why the net active count must return to zero once all workers finish.
				m.AddActive(1)

				if i%10 == 0 {
					m.IncErrors()
				}

				// Exercising different bucket paths concurrently.
				if i%2 == 0 {
					// Bucket 0
					m.ObserveLatency(4 * time.Millisecond)
				} else {
					// Bucket 7 (Overflow)
					m.ObserveLatency(600 * time.Millisecond)
				}

				m.AddActive(-1)

				// Concurrent readers: taking snapshots while writers are actively incrementing fields; moreover, this proves Load() does not race or tear against Add().
				if i%100 == 0 {
					_ = m.Snapshot()
				}
			}
		})
	}

	// Block until all 50 workers finish their 1000 operations so that the race detector evaluates all concurrent memory accesses under real contention.
	wg.Wait()

	snap := m.Snapshot()

	expectedRequests := uint64(numWorkers * iterations)
	if snap.RequestsTotal != expectedRequests {
		t.Fatalf("expected RequestsTotal %d, got %d", expectedRequests, snap.RequestsTotal)
	}

	expectedErrors := uint64(numWorkers * (iterations / 10))
	if snap.ErrorsTotal != expectedErrors {
		t.Fatalf("expected ErrorsTotal %d, got %d", expectedErrors, snap.ErrorsTotal)
	}

	// All workers balanced their AddActive(+1) with AddActive(-1); that's the reason ActiveRequests must be strictly 0.
	if snap.ActiveRequests != 0 {
		t.Fatalf("expected ActiveRequests 0, got %d", snap.ActiveRequests)
	}
}

// Verifies that latency categorization achieves strictly 0 B/op and 0 allocations/op.
func BenchmarkObserveLatency(b *testing.B) {
	m := zeroallocmetrics.New()

	// b.ReportAllocs tracks heap memory allocations per operation so that we can deterministically confirm zero-allocation hot paths.
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		m.ObserveLatency(15 * time.Millisecond)
	}
}

// Evaluates the raw throughput and execution time of atomic counter increments.
func BenchmarkIncRequests(b *testing.B) {
	m := zeroallocmetrics.New()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		m.IncRequests()
	}
}

// Measures the cost of creating an immutable value copy of all metrics.
func BenchmarkSnapshot(b *testing.B) {
	m := zeroallocmetrics.New()

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_ = m.Snapshot()
	}
}
