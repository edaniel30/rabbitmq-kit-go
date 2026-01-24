package circuitbreaker

import (
	"fmt"
	"sync"
	"time"
)

// State represents the circuit breaker state.
type State int

const (
	// StateClosed means the circuit is closed and requests are allowed.
	StateClosed State = iota

	// StateOpen means the circuit is open and requests are blocked.
	StateOpen

	// StateHalfOpen means the circuit is testing if the service has recovered.
	StateHalfOpen
)

// String returns the string representation of the state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern.
//
// It protects consumers from cascading failures by temporarily blocking
// message processing when error rates are too high.
//
// States:
//   - Closed: Normal operation, all messages processed
//   - Open: Too many failures, all messages rejected for ResetTimeout
//   - Half-Open: Testing recovery, limited messages allowed
//
// Example:
//
//	cb := circuitbreaker.New(circuitbreaker.DefaultConfig())
//
//	// Before processing message
//	if !cb.AllowRequest() {
//	    // Circuit is open, reject message
//	    return errors.New("circuit breaker open")
//	}
//
//	// Process message
//	err := processMessage(msg)
//
//	// Record result
//	if err != nil {
//	    cb.RecordFailure()
//	} else {
//	    cb.RecordSuccess()
//	}
type CircuitBreaker struct {
	config Config

	mu                  sync.RWMutex
	state               State
	failures            int
	successes           int
	lastFailureTime     time.Time
	lastStateChangeTime time.Time
	halfOpenRequests    int
}

// New creates a new CircuitBreaker with the given configuration.
func New(config Config) *CircuitBreaker {
	if config.MaxFailures <= 0 {
		config.MaxFailures = 5
	}
	if config.ResetTimeout <= 0 {
		config.ResetTimeout = 60 * time.Second
	}
	if config.HalfOpenMaxRequests <= 0 {
		config.HalfOpenMaxRequests = 3
	}

	return &CircuitBreaker{
		config:              config,
		state:               StateClosed,
		lastStateChangeTime: time.Now(),
	}
}

// AllowRequest returns true if the request should be allowed.
//
// Returns false if the circuit is open (too many failures).
func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		// Always allow in closed state
		return true

	case StateOpen:
		// Check if enough time has passed to try half-open
		if now.Sub(cb.lastStateChangeTime) >= cb.config.ResetTimeout {
			cb.setState(StateHalfOpen)
			cb.halfOpenRequests = 0
			cb.successes = 0
			cb.failures = 0
			return true
		}
		// Still in open state, reject
		return false

	case StateHalfOpen:
		// Allow limited requests in half-open state
		if cb.halfOpenRequests < cb.config.HalfOpenMaxRequests {
			cb.halfOpenRequests++
			return true
		}
		return false

	default:
		return false
	}
}

// RecordSuccess records a successful request.
//
// In half-open state, this may transition to closed if enough successes.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		// Reset failure counter on success
		cb.failures = 0

	case StateHalfOpen:
		cb.successes++
		// If we have enough successes in half-open, close the circuit
		if cb.successes >= cb.config.HalfOpenMaxRequests {
			cb.setState(StateClosed)
			cb.failures = 0
			cb.successes = 0
			cb.halfOpenRequests = 0
		}

	case StateOpen:
		// Success in open state doesn't affect anything (circuit stays open)
	}
}

// RecordFailure records a failed request.
//
// May transition to open state if too many consecutive failures.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		// Check if we should open the circuit
		if cb.failures >= cb.config.MaxFailures {
			cb.setState(StateOpen)
		}

	case StateHalfOpen:
		// Any failure in half-open goes back to open
		cb.setState(StateOpen)
		cb.halfOpenRequests = 0
		cb.successes = 0

	case StateOpen:
		// Already open, failures just accumulate
	}
}

// GetState returns the current state of the circuit breaker.
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetMetrics returns current metrics for monitoring.
func (cb *CircuitBreaker) GetMetrics() Metrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return Metrics{
		State:               cb.state,
		Failures:            cb.failures,
		Successes:           cb.successes,
		HalfOpenRequests:    cb.halfOpenRequests,
		LastFailureTime:     cb.lastFailureTime,
		LastStateChangeTime: cb.lastStateChangeTime,
	}
}

// Reset manually resets the circuit breaker to closed state.
//
// This should be used cautiously, typically only for manual intervention.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	oldState := cb.state
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenRequests = 0
	cb.lastStateChangeTime = time.Now()

	if cb.config.OnStateChange != nil && oldState != StateClosed {
		cb.config.OnStateChange(oldState, StateClosed)
	}
}

// setState changes the state and calls the callback if configured.
func (cb *CircuitBreaker) setState(newState State) {
	oldState := cb.state
	cb.state = newState
	cb.lastStateChangeTime = time.Now()

	if cb.config.OnStateChange != nil && oldState != newState {
		// Call callback without holding lock to avoid deadlock
		go cb.config.OnStateChange(oldState, newState)
	}
}

// Metrics contains circuit breaker metrics for monitoring.
type Metrics struct {
	State               State
	Failures            int
	Successes           int
	HalfOpenRequests    int
	LastFailureTime     time.Time
	LastStateChangeTime time.Time
}

// String returns a string representation of the metrics.
func (m Metrics) String() string {
	return fmt.Sprintf(
		"CircuitBreaker[state=%s, failures=%d, successes=%d, halfOpen=%d]",
		m.State, m.Failures, m.Successes, m.HalfOpenRequests,
	)
}
