package circuitbreaker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestState_String(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{"closed state", StateClosed, "closed"},
		{"open state", StateOpen, "open"},
		{"half-open state", StateHalfOpen, "half-open"},
		{"unknown state", State(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.state.String())
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, 5, cfg.MaxFailures)
	assert.Equal(t, 60*time.Second, cfg.ResetTimeout)
	assert.Equal(t, 3, cfg.HalfOpenMaxRequests)
	assert.Nil(t, cfg.OnStateChange)
}

func TestNew(t *testing.T) {
	t.Run("creates circuit breaker with valid config", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         10,
			ResetTimeout:        30 * time.Second,
			HalfOpenMaxRequests: 5,
		}

		cb := New(cfg)

		assert.NotNil(t, cb)
		assert.Equal(t, StateClosed, cb.GetState())
		assert.Equal(t, 10, cb.config.MaxFailures)
		assert.Equal(t, 30*time.Second, cb.config.ResetTimeout)
		assert.Equal(t, 5, cb.config.HalfOpenMaxRequests)
	})

	t.Run("applies defaults for zero values", func(t *testing.T) {
		cfg := Config{} // All zero values

		cb := New(cfg)

		assert.NotNil(t, cb)
		assert.Equal(t, 5, cb.config.MaxFailures)
		assert.Equal(t, 60*time.Second, cb.config.ResetTimeout)
		assert.Equal(t, 3, cb.config.HalfOpenMaxRequests)
	})

	t.Run("starts in closed state", func(t *testing.T) {
		cb := New(DefaultConfig())
		assert.Equal(t, StateClosed, cb.GetState())
	})
}

func TestCircuitBreaker_AllowRequest(t *testing.T) {
	t.Run("allows requests when closed", func(t *testing.T) {
		cb := New(DefaultConfig())
		assert.True(t, cb.AllowRequest())
	})

	t.Run("blocks requests when open", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        1 * time.Hour, // Long timeout so it stays open
			HalfOpenMaxRequests: 1,
		}
		cb := New(cfg)

		// Record enough failures to open circuit
		cb.RecordFailure()
		cb.RecordFailure()

		assert.Equal(t, StateOpen, cb.GetState())
		assert.False(t, cb.AllowRequest())
	})

	t.Run("transitions to half-open after timeout", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        100 * time.Millisecond,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, StateOpen, cb.GetState())

		// Wait for reset timeout
		time.Sleep(150 * time.Millisecond)

		// Should now allow requests (half-open)
		assert.True(t, cb.AllowRequest())
		assert.Equal(t, StateHalfOpen, cb.GetState())
	})

	t.Run("transitions to half-open and allows limited requests", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        100 * time.Millisecond,
			HalfOpenMaxRequests: 3,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()

		// Wait to enter half-open
		time.Sleep(150 * time.Millisecond)

		// First request transitions to half-open
		assert.True(t, cb.AllowRequest())
		assert.Equal(t, StateHalfOpen, cb.GetState())
	})

	t.Run("half-open blocks requests after max limit reached", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        50 * time.Millisecond,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, StateOpen, cb.GetState())

		// Wait to enter half-open
		time.Sleep(100 * time.Millisecond)

		// First request transitions to half-open (doesn't count toward limit)
		allowed1 := cb.AllowRequest()
		assert.True(t, allowed1)
		assert.Equal(t, StateHalfOpen, cb.GetState())

		// These count toward the limit
		allowed2 := cb.AllowRequest()
		assert.True(t, allowed2)

		allowed3 := cb.AllowRequest()
		assert.True(t, allowed3)

		// Should block after reaching max (2 requests in half-open)
		allowed4 := cb.AllowRequest()
		assert.False(t, allowed4)
		assert.Equal(t, StateHalfOpen, cb.GetState())
	})
}

func TestCircuitBreaker_RecordSuccess(t *testing.T) {
	t.Run("success in closed state keeps circuit closed", func(t *testing.T) {
		cb := New(DefaultConfig())

		cb.RecordSuccess()

		assert.Equal(t, StateClosed, cb.GetState())
		metrics := cb.GetMetrics()
		assert.Equal(t, 0, metrics.Failures)
		// Successes are only tracked in half-open state
		assert.Equal(t, 0, metrics.Successes)
	})

	t.Run("success in half-open closes circuit after enough successes", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        100 * time.Millisecond,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, StateOpen, cb.GetState())

		// Wait to enter half-open
		time.Sleep(150 * time.Millisecond)
		cb.AllowRequest() // Transition to half-open

		// Record successes
		cb.RecordSuccess()
		cb.RecordSuccess()

		// Should transition back to closed
		assert.Equal(t, StateClosed, cb.GetState())
	})

	t.Run("resets failure count on success", func(t *testing.T) {
		cb := New(DefaultConfig())

		cb.RecordFailure()
		metrics := cb.GetMetrics()
		assert.Equal(t, 1, metrics.Failures)

		cb.RecordSuccess()
		metrics = cb.GetMetrics()
		assert.Equal(t, 0, metrics.Failures)
		// Successes are only tracked in half-open state
		assert.Equal(t, 0, metrics.Successes)
	})
}

func TestCircuitBreaker_RecordFailure(t *testing.T) {
	t.Run("failure increments counter", func(t *testing.T) {
		cb := New(DefaultConfig())

		cb.RecordFailure()

		metrics := cb.GetMetrics()
		assert.Equal(t, 1, metrics.Failures)
	})

	t.Run("opens circuit after max failures", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         3,
			ResetTimeout:        60 * time.Second,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Record failures below threshold
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, StateClosed, cb.GetState())

		// One more failure should open circuit
		cb.RecordFailure()
		assert.Equal(t, StateOpen, cb.GetState())
	})

	t.Run("failure in half-open reopens circuit", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        100 * time.Millisecond,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()

		// Wait to enter half-open
		time.Sleep(150 * time.Millisecond)
		cb.AllowRequest()
		assert.Equal(t, StateHalfOpen, cb.GetState())

		// Record failure in half-open
		cb.RecordFailure()

		// Should go back to open
		assert.Equal(t, StateOpen, cb.GetState())
	})
}

func TestCircuitBreaker_GetState(t *testing.T) {
	cb := New(DefaultConfig())

	state := cb.GetState()
	assert.Equal(t, StateClosed, state)
}

func TestCircuitBreaker_GetMetrics(t *testing.T) {
	cb := New(DefaultConfig())

	// Initial metrics
	metrics := cb.GetMetrics()
	assert.Equal(t, StateClosed, metrics.State)
	assert.Equal(t, 0, metrics.Failures)
	assert.Equal(t, 0, metrics.Successes)
	assert.NotZero(t, metrics.LastStateChangeTime)

	// After some activity
	cb.RecordFailure()
	cb.RecordSuccess()

	metrics = cb.GetMetrics()
	assert.Equal(t, 0, metrics.Failures) // Reset on success
	// Successes only tracked in half-open
	assert.Equal(t, 0, metrics.Successes)
}

func TestCircuitBreaker_Reset(t *testing.T) {
	t.Run("reset closes open circuit", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        60 * time.Second,
			HalfOpenMaxRequests: 2,
		}
		cb := New(cfg)

		// Open the circuit
		cb.RecordFailure()
		cb.RecordFailure()
		assert.Equal(t, StateOpen, cb.GetState())

		// Reset
		cb.Reset()

		assert.Equal(t, StateClosed, cb.GetState())
		metrics := cb.GetMetrics()
		assert.Equal(t, 0, metrics.Failures)
		assert.Equal(t, 0, metrics.Successes)
	})

	t.Run("reset clears counters", func(t *testing.T) {
		cb := New(DefaultConfig())

		cb.RecordFailure()
		cb.RecordSuccess()

		cb.Reset()

		metrics := cb.GetMetrics()
		assert.Equal(t, 0, metrics.Failures)
		assert.Equal(t, 0, metrics.Successes)
	})

	t.Run("reset from closed state doesn't trigger callback", func(t *testing.T) {
		var called bool
		cfg := Config{
			MaxFailures:         5,
			ResetTimeout:        60 * time.Second,
			HalfOpenMaxRequests: 2,
			OnStateChange: func(from, to State) {
				called = true
			},
		}
		cb := New(cfg)

		// Already in closed state
		assert.Equal(t, StateClosed, cb.GetState())

		// Reset from closed
		cb.Reset()

		// Callback should not be called
		assert.False(t, called)
		assert.Equal(t, StateClosed, cb.GetState())
	})
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	t.Run("works without callback", func(t *testing.T) {
		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        60 * time.Second,
			HalfOpenMaxRequests: 2,
			OnStateChange:       nil, // No callback
		}
		cb := New(cfg)

		// Should not panic
		assert.NotPanics(t, func() {
			cb.RecordFailure()
			cb.RecordFailure()
		})

		assert.Equal(t, StateOpen, cb.GetState())
	})

	t.Run("calls callback on state change", func(t *testing.T) {
		done := make(chan bool, 1)
		var from, to State

		cfg := Config{
			MaxFailures:         2,
			ResetTimeout:        60 * time.Second,
			HalfOpenMaxRequests: 2,
			OnStateChange: func(fromState, toState State) {
				from = fromState
				to = toState
				done <- true
			},
		}
		cb := New(cfg)

		// Trigger state change
		cb.RecordFailure()
		cb.RecordFailure()

		// Wait for callback to be called (with timeout)
		select {
		case <-done:
			assert.Equal(t, StateClosed, from)
			assert.Equal(t, StateOpen, to)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("callback was not called")
		}
	})
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	t.Run("safe for concurrent use", func(t *testing.T) {
		cb := New(DefaultConfig())

		done := make(chan bool)

		// Concurrent readers
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					cb.AllowRequest()
					cb.GetState()
					cb.GetMetrics()
				}
				done <- true
			}()
		}

		// Concurrent writers
		for i := 0; i < 10; i++ {
			go func() {
				for j := 0; j < 100; j++ {
					if j%2 == 0 {
						cb.RecordSuccess()
					} else {
						cb.RecordFailure()
					}
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 20; i++ {
			<-done
		}

		// Should not panic and should have valid state
		state := cb.GetState()
		assert.True(t, state == StateClosed || state == StateOpen || state == StateHalfOpen)
	})
}

func TestMetrics_String(t *testing.T) {
	cb := New(DefaultConfig())
	cb.RecordFailure()
	cb.RecordSuccess()

	metrics := cb.GetMetrics()
	str := metrics.String()

	// The actual format is: "CircuitBreaker[state=closed, failures=0, successes=0, halfOpen=0]"
	assert.Contains(t, str, "state=")
	assert.Contains(t, str, "closed")
	assert.Contains(t, str, "failures=")
	assert.Contains(t, str, "successes=")
}
