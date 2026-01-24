package circuitbreaker

import "time"

// Config holds the configuration for the circuit breaker.
type Config struct {
	// MaxFailures is the number of consecutive failures before opening the circuit.
	// Default: 5
	MaxFailures int

	// ResetTimeout is how long to wait in open state before attempting half-open.
	// Default: 60 seconds
	ResetTimeout time.Duration

	// HalfOpenMaxRequests is the number of requests to allow in half-open state.
	// Default: 3
	HalfOpenMaxRequests int

	// OnStateChange is called when the circuit breaker changes state.
	// Optional callback for monitoring/alerting.
	OnStateChange func(from, to State)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxFailures:         5,
		ResetTimeout:        60 * time.Second,
		HalfOpenMaxRequests: 3,
	}
}
