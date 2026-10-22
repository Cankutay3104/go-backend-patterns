package zeroallocmetrics

import (
	"sync/atomic"
	"time"
)

// NumBuckets defines the total count of fixed latency buckets.
const NumBuckets = 8

// DefaultBucketUpperBounds defines fixed latency thresholds in ascending order: <= 5ms, <= 10ms, <= 25ms, <= 50ms, <= 100ms, <= 250ms, <= 500ms, > 500ms (Inf)
var DefaultBucketUpperBounds = [NumBuckets - 1]time.Duration{
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
}

// Metrics tracks service telemetry using lock-free atomic primitives.
type Metrics struct {
	requestsTotal  atomic.Uint64
	errorsTotal    atomic.Uint64
	activeRequests atomic.Int64
	latencyBuckets [NumBuckets]atomic.Uint64
}

// New initializes a zero-allocation metrics collector
func New() *Metrics {
	return &Metrics{}
}

// IncRequests atomically increments total processed requests by 1.
func (m *Metrics) IncRequests() {
	m.requestsTotal.Add(1)
}

// IncErrors atomically increments total error occurrences by 1.
func (m *Metrics) IncErrors() {
	m.errorsTotal.Add(1)
}

// AddActive atomically adjusts in-flight request gauge (+1 on entry, -1 on exit).
func (m *Metrics) AddActive(difference int64) {
	m.activeRequests.Add(difference)
}

// ObserveLatency categorizes an aobserved duration into a static histogram bucket. It scans fixed compile-time thresholds without allocating dynamic slices or sorting that's why the latency tracking remains strictly zero-allocation.
func (m *Metrics) ObserveLatency(dur time.Duration) {
	for i, bound := range DefaultBucketUpperBounds {
		if dur <= bound {
			m.latencyBuckets[i].Add(1)
			return
		}
	}
	// Duration exceeded all thresholds (> 500ms), so we record it into the final bucket.
	m.latencyBuckets[NumBuckets-1].Add(1)
}

// Snapshot holds an immutable point-in-time value copy of collected metrics
type Snapshot struct {
	RequestsTotal  uint64
	ErrorsTotal    uint64
	ActiveRequests int64
	LatencyBuckets [NumBuckets]uint64
}

// Snapshot exports an immutable point-in-time value copy of all metrics. It loads atomic values without acquiring locks; furthermore, concurrent writers on the hot request path are never blocked.
func (m *Metrics) Snapshot() Snapshot {
	var snapshot Snapshot

	snapshot.RequestsTotal = m.requestsTotal.Load()
	snapshot.ErrorsTotal = m.errorsTotal.Load()
	snapshot.ActiveRequests = m.activeRequests.Load()

	for i := range NumBuckets {
		snapshot.LatencyBuckets[i] = m.latencyBuckets[i].Load()
	}

	return snapshot
}
