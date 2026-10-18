package retrybackoff_test

import (
	"context"
	"errors"
	"testing"
	"time"

	retrybackoff "go-backend-patterns/resiliency/retry_backoff"
)

var (
	errTransient = errors.New("transient downstream timeout")
	errPermanent = errors.New("permanent validation error")
)

// Verifies that a healthy call executes once and returns nil.
func TestExecute_ImmediateSuccess(t *testing.T) {
	ctx := context.Background()
	cfg := retrybackoff.Config{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if execCount != 1 {
		t.Fatalf("execution count mismatch: expected 1, got %d", execCount)
	}
}

// Ensures non-retryable errors fail fast without consuming retry budget.
func TestExecute_PredicateFiltering(t *testing.T) {
	ctx := context.Background()
	cfg := retrybackoff.Config{
		MaxAttempts: 5,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  100 * time.Millisecond,
	}

	// Predicate only allows transient errors to retry; that's the reason permanent errors abort immediately.
	isRetryable := func(err error) bool {
		return errors.Is(err, errTransient)
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, isRetryable, func(ctx context.Context) error {
		execCount++
		return errPermanent
	})

	if !errors.Is(err, errPermanent) {
		t.Fatalf("expected errPermanent, got %v", err)
	}
	// Proving it did not attempt to retry.
	if execCount != 1 {
		t.Fatalf("execution count mismatch: expected 1, got %d", execCount)
	}
}

// Verifies that failing operations are retried up to MaxAttempts.
func TestExecute_RetryLimitExhaustion(t *testing.T) {
	ctx := context.Background()
	cfg := retrybackoff.Config{
		MaxAttempts: 3,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		return errTransient
	})

	if !errors.Is(err, errTransient) {
		t.Fatalf("expected errTransient, got %v", err)
	}
	if execCount != 3 {
		t.Fatalf("execution count mismatch: expected 3, got %d", execCount)
	}
}

// Verifies recovery when a call fails initially and then succeeds.
func TestExecute_EventuallySucceeds(t *testing.T) {
	ctx := context.Background()
	cfg := retrybackoff.Config{
		MaxAttempts: 4,
		BaseBackoff: 2 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		if execCount < 3 {
			return errTransient
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if execCount != 3 {
		t.Fatalf("execution count mismatch: expected 3, got %d", execCount)
	}
}

// Proves that backoff timers yield immediately when the parent context is cancelled, preventing goroutine leaks.
func TestExecute_ContextCancellationDuringBackoff(t *testing.T) {
	// A long backoff ensures the test will block if context cancellation doesn't interrupt it.
	cfg := retrybackoff.Config{
		MaxAttempts: 5,
		BaseBackoff: 500 * time.Millisecond,
		MaxBackoff:  2 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	execCount := 0
	start := time.Now()

	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		// Cancel the context during the first failure; therefore, the subsequent backoff select must catch ctx.Done() immediately.
		cancel()
		return errTransient
	})

	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if execCount != 1 {
		t.Fatalf("execution count mismatch: expected 1, got %d", execCount)
	}
	// If timer cleanup failed, elapsed time would be >= 500ms.
	if elapsed > 150*time.Millisecond {
		t.Fatalf("backoff did not abort promptly: elapsed %v", elapsed)
	}
}

// Verifies the defensive pre-flight check when context is already dead.
func TestExecute_ContextPreCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Context is cancelled before invocation.

	cfg := retrybackoff.Config{
		MaxAttempts: 3,
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  50 * time.Millisecond,
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	// For that purpose, fn must not be invoked if the context is already dead.
	if execCount != 0 {
		t.Fatalf("fn was invoked despite pre-cancelled context: count %d", execCount)
	}
}

// Ensures invalid or zero configurations do not panic or loop infinitely.
func TestExecute_ConfigNormalization(t *testing.T) {
	ctx := context.Background()
	// Zero MaxAttempts and negative backoffs.
	cfg := retrybackoff.Config{
		MaxAttempts: 0,
		BaseBackoff: -5 * time.Millisecond,
		MaxBackoff:  -10 * time.Millisecond,
	}

	execCount := 0
	err := retrybackoff.Execute(ctx, cfg, retrybackoff.AlwaysRetry, func(ctx context.Context) error {
		execCount++
		return nil
	})

	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	// Defensively clamped to at least 1 attempt.
	if execCount != 1 {
		t.Fatalf("expected normalized MaxAttempts to execute once, got %d", execCount)
	}
}
