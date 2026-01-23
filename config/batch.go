package config

// BatchOption is a functional option for configuring batch publish operations.
type BatchOption func(*BatchConfig)

// batchConfig holds configuration for batch publish operations.
type BatchConfig struct {
	UsePipelining  bool // Use pipelining (send all, then wait confirms). Default: true
	FailFast       bool // Stop on first error. Default: false (collect all errors)
	MaxConcurrency int  // Max concurrent workers for async. Default: 0 (unlimited)
}

// defaultBatchConfig returns the default batch configuration.
func DefaultBatchConfig() BatchConfig {
	return BatchConfig{
		UsePipelining:  true,
		FailFast:       false,
		MaxConcurrency: 0,
	}
}

// WithPipelining enables or disables pipelining for batch publishes.
//
// When enabled (default), all messages are sent first without waiting for
// confirmations, then all confirmations are collected. This is much faster
// for large batches (5-10x improvement).
//
// When disabled, messages are published one at a time, waiting for each
// confirmation before sending the next (legacy behavior).
func WithPipelining(enabled bool) BatchOption {
	return func(c *BatchConfig) {
		c.UsePipelining = enabled
	}
}

// WithFailFast enables or disables fail-fast mode for batch publishes.
//
// When enabled, the operation stops at the first error encountered.
// When disabled (default), all messages are attempted and all errors are collected.
func WithFailFast(enabled bool) BatchOption {
	return func(c *BatchConfig) {
		c.FailFast = enabled
	}
}

// WithMaxConcurrency sets the maximum number of concurrent workers for async batch publishes.
//
// Default: 0 (unlimited)
// Recommended: 50-100 for most use cases
func WithMaxConcurrency(n int) BatchOption {
	return func(c *BatchConfig) {
		c.MaxConcurrency = n
	}
}
