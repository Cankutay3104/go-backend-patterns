package tokenbucket_test

import (
	"sync"
	"testing"
	"time"

	tokenbucket "go-backend-patterns/resiliency/token_bucket_limiter"
)

// Verifies that a full bucket permits an immediate burst up to capacity and rejects the subsequent call once drained.
func TestTokenBucket_Burst(t *testing.T) {
	// A lower refill rate of 1 token/sec provides a large safety margin against test jitter, that's the reason why the burst assertions remain stable under high CPU load.
	limiter := tokenbucket.New(1, 5)

	for range 5 {
		if !limiter.Allow() {
			t.Fatalf("expected burst permit to succeed, got false.")
		}
	}

	// 6th call must fail fast because all capacity tokens have been drained.
	if limiter.Allow() {
		t.Fatalf("expected exhausted bucket to reject request, got true")
	}
}

// Proves that tokens accumulate deterministically as a function of elapsed time without using wall-clock sleeps.
func TestTokenBucket_MathematicalReplenishment(t *testing.T) {
	limiter := tokenbucket.New(10, 10)
	// Capturing start after New(); therefore, we avoid clock skew issues during initialization.
	start := time.Now()

	// Draining all 10 tokens at start.
	if !limiter.AllowN(start, 10) {
		t.Fatalf("expected initial burst to succeed, failed.")
	}

	// At start, zero tokens remain; that's why an immediate request must fail.
	if limiter.AllowN(start, 1) {
		t.Fatalf("expected call at start to fail due to empty bucket, succeeded.")
	}

	// Advancing simulated time by 500ms; at 10 tokens/sec, exactly 5 tokens accumulate.
	time1 := start.Add(500 * time.Millisecond)
	if !limiter.AllowN(time1, 5) {
		t.Fatalf("expected 5 replenished tokens to be consumed successfully, failed.")
	}

	// The 5 replenished tokens were consumed; that's why another call at time1 must fail.
	if limiter.AllowN(time1, 1) {
		t.Fatalf("expected immediate call to fail, succeeded.")
	}
}

// Ensures that idle buckets clamp to capacity rather than accumulating tokens indefinitely.
func TestTokenBucket_CapacityCeiling(t *testing.T) {
	limiter := tokenbucket.New(10, 5)
	start := time.Now()

	// Drain 3 tokens, leaving 2 in the bucket.
	if !limiter.AllowN(start, 3) {
		t.Fatalf("expected deduction of 3 tokens to succeed, failed")
	}

	// Advance time by 1 hour; mathematically 36,000 tokens accumulate.
	future := start.Add(1 * time.Hour)

	// Consuming 5 tokens succeeds because the bucket is capped at capacity = 5.
	if !limiter.AllowN(future, 5) {
		t.Fatalf("expected full capacity burst to succeed after long idle window, failed")
	}

	// An immediate 6th token must fail; therefore, we prove the bucket clamped at 5 and did not hold 36,000 tokens.
	if limiter.AllowN(future, 1) {
		t.Fatalf("tokens exceeded capacity")
	}
}

// Verifies that requests demanding more tokens than currently available deduct zero tokens from the balance.
func TestTokenBucket_AllowN_PartialFailure(t *testing.T) {
	limiter := tokenbucket.New(10, 5)
	start := time.Now()

	// Demanding 10 tokens when capacity is 5 must be rejected.
	if limiter.AllowN(start, 10) {
		t.Fatalf("expected request exceeding capacity to fail")
	}

	// The previous call failed; furthermore, it should not have modified the balance. Therefore, the full 5 tokens must still be present.
	if !limiter.AllowN(start, 5) {
		t.Fatalf("expected intact token balance to permit 5 tokens")
	}
}

// Asserts that parallel calls to Allow() across multiple goroutines pass cleanly under the Go race detector without deadlocks or race conditions.
func TestTokenBucket_ConcurrencyRace(t *testing.T) {
	limiter := tokenbucket.New(100, 50)

	var wg sync.WaitGroup

	for range 50 {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for range 20 {
				limiter.Allow()
			}
		}()
	}
	// Block until all 50 goroutines finish their 20 Allow() iterations so that the race detector can analyze concurrent read/write locks under actual contention.
	wg.Wait()
}
