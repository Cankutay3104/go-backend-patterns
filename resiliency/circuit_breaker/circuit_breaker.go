package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

// State represents the operational mode of the circuit breaker Finite State Machine.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

// ErrCircuitOpen is returned immediately when requests are short-circuited in the Open state.
var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitBreaker manages state transitions and guards downstream resources from cascading failure.
type CircuitBreaker struct {
	state     State
	threshold int
	counter   int
	cooldown  time.Duration
	timestamp time.Time
	mutex     sync.Mutex
}

// Constructore for CircuitBreaker
func New(threshold int, cooldown time.Duration) *CircuitBreaker {
	cb := CircuitBreaker{
		state:     0,
		threshold: threshold,
		counter:   0,
		cooldown:  cooldown,
		timestamp: time.Now(),
		mutex:     sync.Mutex{},
	}
	return &cb
}

// Execute wraps a fallible operation fn within the circuit breaker lifecycle. Verifies state prerequisites before dispatching.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	// Pre-flight checks
	cb.mutex.Lock()

	switch cb.state {
	case StateOpen:
		// Check whether the recovery window has elapsed.
		if time.Since(cb.timestamp) > cb.cooldown {
			// Cooldown has elapsed, transition to half-open.
			// This request becomes the probe and breaks out of the switch to call fn().
			cb.state = StateHalfOpen
			break
		}
		// Cooldown still active, so fail fast.
		cb.mutex.Unlock()
		return ErrCircuitOpen

	case StateHalfOpen:
		// A probe is already in flight, we accept only 1 request and reject the rest.
		cb.mutex.Unlock()
		return ErrCircuitOpen

	case StateClosed:
		// Normal traffic flows through freely
		break
	}

	// Releasing the mutex prior to caling external I/O so that concurrent requests do not block on a single slow network call.
	cb.mutex.Unlock()

	// External Invocation
	err := fn()

	// Post-flight transitions
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if err != nil {
		switch cb.state {
		case StateHalfOpen:
			// Probe failed.
			cb.state = StateOpen
			cb.timestamp = time.Now()
		case StateClosed:
			cb.counter++
			// If consecutive failures reach the threshold, trip the circuit breaker.
			if cb.counter >= cb.threshold {
				cb.state = StateOpen
				cb.timestamp = time.Now()
			}
		}
		return err
	}

	// Execution succeeded.
	switch cb.state {
	case StateHalfOpen:
		// Probe succeeded. We mark the dependency healthy again.
		cb.state = StateClosed
		cb.counter = 0
	case StateClosed:
		// Reset consecutive failure counter on any successful execution.
		cb.counter = 0
	}

	return nil
}
