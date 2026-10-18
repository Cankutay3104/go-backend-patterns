package retrybackoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Config defines the retry constraints and backoff intervals.
type Config struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// Evaluates whether a given error qualifies for retry attempts.
type RetryableFunc func(err error) bool

// AlwaysRetry is a convenience predicate that marks all non-nil errors as retryable; for that purpose, callers do not need to redeclare an inline predicate for simple retries.
func AlwaysRetry(err error) bool {
	return err != nil
}

// Wraps the execution of fn within an exponential backoff retry loop using Full Jitter. Watches for context cancellation and ensures timers are cleared promptly upon early return.
func Execute(ctx context.Context, cfg Config, isRetryable RetryableFunc, fn func(ctx context.Context) error) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.BaseBackoff < 0 {
		cfg.BaseBackoff = 0
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = cfg.BaseBackoff
	}

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		// Pre-flight cancellation check: failing fast if context is expired.
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		// Non-retryable errors abort immediately.
		if isRetryable != nil && !isRetryable(err) {
			return err
		}

		// If this is the final attempt, return the error without sleeping.
		if attempt == cfg.MaxAttempts-1 {
			return err
		}

		// Using bitwise shift operator to calculate exponential multiplier with overflow protection.
		multiplier := 1 << attempt
		if attempt > 30 || multiplier <= 0 {
			multiplier = 1 << 30
		}

		backoff := cfg.BaseBackoff * time.Duration(multiplier)
		if backoff > cfg.MaxBackoff || backoff < 0 {
			backoff = cfg.MaxBackoff
		}

		// Full Jitter: picking a uniform random duration between 0 and backoff. Furthermore, guarding against non-positive durations preventes rand.N from panicking.
		var sleepDuration time.Duration
		if backoff > 0 {
			sleepDuration = rand.N(backoff)
		}

		timer := time.NewTimer(sleepDuration)

		select {
		case <-ctx.Done():
			// Stopping and defensively draining timer to prevent resource leaks.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
			// Timer expired naturally, loop continues to the next attempt.
		}
	}
	return nil
}
