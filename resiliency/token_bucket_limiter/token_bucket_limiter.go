package tokenbucket

import (
	"errors"
	"sync"
	"time"
)

// ErrRateLimitExceeded is returned when the bucket lacks sufficient tokens for a request.
var ErrRateLimitExceeded = errors.New("rate limit exceeded: bucket empty")

// Limiter controls event execution rates using a thread-safe token bucket algorithm.
type Limiter struct {
	mutex      sync.Mutex
	rate       float64   // Refill rate in tokens per second.
	capacity   float64   // Maximum burst ceiling (bucket depth).
	tokens     float64   // Current available token balance.
	lastRefill time.Time // Timestamp of the most recent token calculation.
}

// New initializes a Limiter with a sustained refill rate (tokens/sec) and burst capacity. The bucket starts completely full to allow an initial burst up to capacity.
func New(rate float64, capacity float64) *Limiter {
	limiter := Limiter{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity,
		lastRefill: time.Now(),
	}
	return &limiter
}

// Allow is shorthand for AllowN(time.Now(), 1). It reports whether 1 token may be consumed.
func (l *Limiter) Allow() bool {
	return l.AllowN(time.Now(), 1)
}

// AllowN reports whether n tokens can be consumed at time 'now'.
// If available, tokens are deducted atomically and it returns true; otherwise, it returns false.
func (l *Limiter) AllowN(now time.Time, n float64) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Calculate elapsed time since the last token recalculation.
	elapsedTime := now.Sub(l.lastRefill).Seconds()

	// Replenish tokens if time has moved forward.
	if elapsedTime > 0 {
		// Tokens accumulate based on elapsed seconds and refill rate, capped at capacity; that's why idle buckets do not grow indefinitely.
		l.tokens = min(l.capacity, l.tokens+(elapsedTime*l.rate))
		l.lastRefill = now
	}

	// Evaluate if sufficient quota exists for this request.
	if l.tokens >= n {
		// Deducting tokens for the allowed request; therefore subsequent calls see a reduced balance until time refills it.
		l.tokens -= n
		return true
	}
	// Insufficient available tokens; that's the reason why the request gets rejected.
	return false
}
