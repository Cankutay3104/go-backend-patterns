package circuitbreaker_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	circuitbreaker "go-backend-patterns/resiliency/circuit_breaker"
)

// errDownstream represents a simulated downstream network or dependency failure.
var errDownstream = errors.New("downstream service failure")

// Verifies that N consecutive errors trip the breaker and the N+1th  request fails fast without running the enclosed function.
func TestCircuitBreaker_Tripping(t *testing.T) {
	cb := circuitbreaker.New(3, 1*time.Minute)

	execCount := 0

	// Triggering 3 consecutive failures to hit the threshold.
	for range 3 {
		err := cb.Execute(func() error {
			execCount++
			return errDownstream
		})

		if !errors.Is(err, errDownstream) {
			t.Fatalf("error mismatch: expected %v, got %v", errDownstream, err)
		}
	}

	if execCount != 3 {
		t.Fatalf("execution count is not matching after N failing calls.")
	}

	// The 4th call must fail fast; for that purpose, fn shouldn't be invoked.
	err := cb.Execute(func() error {
		execCount++
		return errDownstream
	})
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("error mismatch: expected %v, got %v", circuitbreaker.ErrCircuitOpen, err)
	}
	if execCount != 3 {
		t.Fatalf("execution count mismatch: expected 3, got %d", execCount)
	}
}

// Ensures that intermittent errors do not trip the breaker if an inteleaved call succeeds, resetting the consecutive counter back to zero.
func TestCircuitBreaker_ResetOnSuccess(t *testing.T) {
	cb := circuitbreaker.New(3, 1*time.Minute)
	execCount := 0

	// 2 consecutive failures.
	for range 2 {
		err := cb.Execute(func() error {
			execCount++
			return errDownstream
		})
		if !errors.Is(err, errDownstream) {
			t.Fatalf("error mismatch: expected %v, got %v", errDownstream, err)
		}
	}

	// 1 successful call, which makes the internal failure counter resets to 0.
	err := cb.Execute(func() error {
		execCount++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// 2 more failures; counter reaches 2, which is still below the threshold of 3.
	for range 2 {
		err := cb.Execute(func() error {
			execCount++
			return errDownstream
		})
		if !errors.Is(err, errDownstream) {
			t.Fatalf("error mismatch: expected %v, got %v", errDownstream, err)
		}
	}

	// 6th call succeeds; that's the reason we know the breaker stayed in StateClosed.
	err = cb.Execute(func() error {
		execCount++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	// Total executed calls must be exactly 6.
	if execCount != 6 {
		t.Fatalf("execution count mismatch: expected 6, got %d", execCount)
	}
}

// Verifies the full recovery cycle: StateClosed -> StateOpen -> Cooldown Expires -> StateHalfOpen -> StateClosed.
func TestCircuitBreaker_Recovery(t *testing.T) {
	cooldown := 40 * time.Millisecond
	cb := circuitbreaker.New(3, cooldown)
	execCount := 0

	// Trip the breaker to StateOpen with 3 consecutive failures.
	for range 3 {
		_ = cb.Execute(func() error {
			execCount++
			return errDownstream
		})
	}

	// Breaker is open; therefore, this call must fail fast without incrementing execCount.
	err := cb.Execute(func() error {
		execCount++
		return nil
	})
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if execCount != 3 {
		t.Fatalf("execution count mismatch after trip: expected 3, got %d", execCount)
	}

	// Waiting for the cooldown window to elapse.
	time.Sleep(cooldown + 10*time.Millisecond)

	// Probe: transitions to StateHalfOpen, runs, and succeeds. Therefore, the state snaps back to StateClosed.
	err = cb.Execute(func() error {
		execCount++
		return nil
	})
	if err != nil {
		t.Fatalf("expected probe to succeed, got %v", err)
	}

	// Normal throughput is restored; subsequent calls proceed without issuee.
	err = cb.Execute(func() error {
		execCount++
		return nil
	})
	if err != nil {
		t.Fatalf("expected subsequent request to succeed, got %v", err)
	}

	// Exactly 5 invocations reachede fn (3 initial failures + 1 probe + 1 normal call).
	if execCount != 5 {
		t.Fatalf("execution count mismatch: expected 5, got %d", execCount)
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cooldown := 40 * time.Millisecond
	cb := circuitbreaker.New(3, cooldown)
	execCount := 0

	// Trip the breaker to StateOpen with 3 consecutive failures.
	for range 3 {
		_ = cb.Execute(func() error {
			execCount++
			return errDownstream
		})
	}

	// Verify breaker is open; this call fails fast without invoking fn.
	err := cb.Execute(func() error {
		execCount++
		return errDownstream
	})
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if execCount != 3 {
		t.Fatalf("execution count mismatch: expected 3, got %d", execCount)
	}

	time.Sleep(cooldown + 10*time.Millisecond)

	// Dispatch the probe. It runs, fails downstream, and immediately trips back to Open.
	err = cb.Execute(func() error {
		execCount++
		return errDownstream
	})
	if !errors.Is(err, errDownstream) {
		t.Fatalf("expected errDownstream, got %v", err)
	}
	if execCount != 4 {
		t.Fatalf("execution count mismatch: expected 4, got %d", execCount)
	}

	// Verify the breaker immediately snapped back to StateOpen on sentence one. For that purpose, the very next call must fail fast without running fn.
	err = cb.Execute(func() error {
		execCount++
		return nil
	})
	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if execCount != 4 {
		t.Fatalf("execution count mismatch: expected 4, got %d", execCount)
	}
}

func TestCircuitBreaker_ConcurrencyRace(t *testing.T) {
	cb := circuitbreaker.New(5, 20*time.Millisecond)

	var wg sync.WaitGroup

	for i := range 50 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Alternating success and failure, or rapid calls
			_ = cb.Execute(func() error {
				if id%2 == 0 {
					return errDownstream
				}
				return nil
			})
		}(i)
	}
	wg.Wait()
}
