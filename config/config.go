package config

import (
	"fmt"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
)

// Config holds the configuration for the RabbitMQ client.
type Config struct {
	// Required fields
	URI string // RabbitMQ connection URI (e.g., "amqp://user:pass@host:5672/")

	// Optional fields with defaults
	ReconnectDelay    time.Duration // Delay between reconnection attempts (default: 5s)
	Timeout           time.Duration // Default timeout for operations (default: 10s)
	PrefetchCount     int           // Number of messages to prefetch (default: 10)
	MaxRetries        int           // Maximum number of retries for failed messages (default: 3)
	PublisherConfirms bool          // Enable publisher confirms for guaranteed delivery (default: false)
	ConfirmTimeout    time.Duration // Timeout for waiting publisher confirms (default: 5s)
	Logger            logger.Logger // Logger for internal logging (default: DefaultLogger)

	// Circuit Breaker configuration
	CircuitBreakerEnabled          bool          // Enable circuit breaker for consumers (default: false)
	CircuitBreakerMaxFailures      int           // Max failures before opening circuit (default: 5)
	CircuitBreakerResetTimeout     time.Duration // Time to wait before attempting half-open (default: 60s)
	CircuitBreakerHalfOpenRequests int           // Max requests in half-open state (default: 3)

	// Dead Letter Queue configuration
	DLQEnabled bool      // Enable automatic DLQ setup (default: false)
	DLQConfig  DLQConfig // DLQ configuration options

	// Topology configuration
	Exchanges []ExchangeConfig // Exchanges to declare on connect
	Queues    []QueueConfig    // Queues to declare and bind on connect
}

// Option is a functional option for configuring the client.
type Option func(*Config)

// DefaultConfig returns a Config with sensible defaults.
//
// Default values:
//   - ReconnectDelay: 5 seconds
//   - Timeout: 10 seconds
//   - PrefetchCount: 10
//   - MaxRetries: 3
//   - Logger: DefaultLogger
//   - CircuitBreakerEnabled: false
//   - CircuitBreakerMaxFailures: 5
//   - CircuitBreakerResetTimeout: 60 seconds
//   - CircuitBreakerHalfOpenRequests: 3
//   - Exchanges: empty
//   - Queues: empty
func DefaultConfig() Config {
	return Config{
		ReconnectDelay:                 5 * time.Second,
		Timeout:                        10 * time.Second,
		PrefetchCount:                  10,
		MaxRetries:                     3,
		Logger:                         logger.NewDefaultLogger(),
		CircuitBreakerEnabled:          false,
		CircuitBreakerMaxFailures:      5,
		CircuitBreakerResetTimeout:     60 * time.Second,
		CircuitBreakerHalfOpenRequests: 3,
		DLQEnabled:                     false,
		DLQConfig:                      DefaultDLQConfig(),
		Exchanges:                      []ExchangeConfig{},
		Queues:                         []QueueConfig{},
	}
}

// validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.URI == "" {
		return errors.NewConfigFieldError("URI", "is required")
	}

	if c.ReconnectDelay <= 0 {
		return errors.NewConfigFieldError("ReconnectDelay", "must be greater than 0")
	}

	if c.Timeout <= 0 {
		return errors.NewConfigFieldError("Timeout", "must be greater than 0")
	}

	if c.PrefetchCount < 0 {
		return errors.NewConfigFieldError("PrefetchCount", "cannot be negative")
	}

	if c.MaxRetries < 0 {
		return errors.NewConfigFieldError("MaxRetries", "cannot be negative")
	}

	// Validate exchanges
	for i, ex := range c.Exchanges {
		if ex.Name == "" {
			return errors.NewConfigFieldError(fmt.Sprintf("Exchanges[%d].Name", i), "is required")
		}
		if ex.Type == "" {
			return errors.NewConfigFieldError(fmt.Sprintf("Exchanges[%d].Type", i), "is required")
		}
		// Validate exchange type
		validTypes := map[string]bool{
			"direct":  true,
			"fanout":  true,
			"topic":   true,
			"headers": true,
		}
		if !validTypes[ex.Type] {
			return errors.NewConfigFieldError(
				fmt.Sprintf("Exchanges[%d].Type", i),
				fmt.Sprintf("must be one of: direct, fanout, topic, headers (got: %s)", ex.Type),
			)
		}
	}

	// Validate queues
	for i, q := range c.Queues {
		if q.Name == "" {
			return errors.NewConfigFieldError(fmt.Sprintf("Queues[%d].Name", i), "is required")
		}
	}

	return nil
}

// WithURI sets the RabbitMQ connection URI.
//
// Example: "amqp://guest:guest@localhost:5672/"
func WithURI(uri string) Option {
	return func(c *Config) {
		c.URI = uri
	}
}

// WithReconnectDelay sets the delay between reconnection attempts.
//
// Default: 5 seconds
func WithReconnectDelay(delay time.Duration) Option {
	return func(c *Config) {
		c.ReconnectDelay = delay
	}
}

// WithTimeout sets the default timeout for operations.
//
// Default: 10 seconds
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// WithPrefetchCount sets the number of messages to prefetch per consumer.
//
// This controls how many unacknowledged messages can be delivered to a consumer.
// Default: 10
func WithPrefetchCount(count int) Option {
	return func(c *Config) {
		c.PrefetchCount = count
	}
}

// WithMaxRetries sets the maximum number of retries for failed messages.
//
// When a message handler returns an error, the message will be retried
// up to MaxRetries times before being discarded or sent to a DLQ.
// Default: 3
func WithMaxRetries(retries int) Option {
	return func(c *Config) {
		c.MaxRetries = retries
	}
}

// WithExchanges sets the exchanges to declare on connection.
//
// These exchanges will be automatically created when the client connects.
// If the exchange already exists, it will be validated against the config.
func WithExchanges(exchanges []ExchangeConfig) Option {
	return func(c *Config) {
		c.Exchanges = exchanges
	}
}

// WithQueues sets the queues to declare and bind on connection.
//
// These queues will be automatically created and bound to their respective
// exchanges when the client connects.
func WithQueues(queues []QueueConfig) Option {
	return func(c *Config) {
		c.Queues = queues
	}
}

// WithPublisherConfirms enables or disables publisher confirms.
//
// When enabled, the client will wait for RabbitMQ to confirm that messages
// have been received and persisted. This provides guaranteed delivery but
// has a performance impact.
//
// Recommended for production environments where message loss is not acceptable.
// Default: false
func WithPublisherConfirms(enabled bool) Option {
	return func(c *Config) {
		c.PublisherConfirms = enabled
	}
}

// WithConfirmTimeout sets the timeout for waiting publisher confirms.
//
// This is only used when PublisherConfirms is enabled.
// Default: 5 seconds
func WithConfirmTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.ConfirmTimeout = timeout
	}
}

// WithLogger sets a custom logger for the RabbitMQ client.
//
// This allows you to inject your own logger implementation (zap, logrus, zerolog, etc.)
// instead of using the default logger.
//
// Example with custom logger:
//
//	type MyLogger struct{}
//
//	func (l *MyLogger) Info(msg string, args ...any) {
//	    // Your logging implementation
//	}
//	// ... implement other methods
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithLogger(&MyLogger{}),
//	)
//
// To disable logging completely, use NoopLogger:
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithLogger(&logger.NoopLogger{}),
//	)
//
// Default: DefaultLogger (writes to stderr with timestamps)
func WithLogger(log logger.Logger) Option {
	return func(c *Config) {
		c.Logger = log
	}
}

// WithCircuitBreaker enables or disables the circuit breaker for consumers.
//
// When enabled, the consumer will automatically stop processing messages
// when error rates are too high, protecting against cascading failures.
//
// The circuit breaker has three states:
//   - Closed: Normal operation, all messages processed
//   - Open: Too many failures, messages are rejected (nacked without requeue)
//   - Half-Open: Testing recovery, limited messages allowed
//
// Example:
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithCircuitBreaker(true),
//	    config.WithCircuitBreakerMaxFailures(10),
//	    config.WithCircuitBreakerResetTimeout(2 * time.Minute),
//	)
//
// Default: false (disabled)
func WithCircuitBreaker(enabled bool) Option {
	return func(c *Config) {
		c.CircuitBreakerEnabled = enabled
	}
}

// WithCircuitBreakerMaxFailures sets the maximum number of consecutive
// failures before opening the circuit.
//
// Default: 5
func WithCircuitBreakerMaxFailures(n int) Option {
	return func(c *Config) {
		c.CircuitBreakerMaxFailures = n
	}
}

// WithCircuitBreakerResetTimeout sets how long to wait in open state
// before attempting to transition to half-open.
//
// Default: 60 seconds
func WithCircuitBreakerResetTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.CircuitBreakerResetTimeout = d
	}
}

// WithCircuitBreakerHalfOpenRequests sets the number of requests to allow
// in half-open state before deciding whether to close or re-open the circuit.
//
// Default: 3
func WithCircuitBreakerHalfOpenRequests(n int) Option {
	return func(c *Config) {
		c.CircuitBreakerHalfOpenRequests = n
	}
}

// WithDLQ enables automatic Dead Letter Queue (DLQ) setup with default configuration.
//
// When enabled, the library will automatically:
//   - Create a Dead Letter Exchange (DLX) named "dlx.exchange"
//   - Create DLQ queues with prefix "dlq." for each main queue
//   - Configure main queues to route failed messages to DLQ
//
// Example:
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithDLQ(true),
//	)
//
// Default: false (disabled)
func WithDLQ(enabled bool) Option {
	return func(c *Config) {
		c.DLQEnabled = enabled
	}
}

// WithCustomDLQ enables DLQ with custom configuration.
//
// This allows full control over DLX naming, queue prefix, exchange type, etc.
//
// Example:
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithCustomDLQ(config.DLQConfig{
//	        ExchangeName: "my.custom.dlx",
//	        QueuePrefix:  "failed.",
//	        ExchangeType: "topic",
//	        Durable:      true,
//	    }),
//	)
func WithCustomDLQ(dlqConfig DLQConfig) Option {
	return func(c *Config) {
		c.DLQEnabled = true
		c.DLQConfig = dlqConfig
	}
}

// WithDLQExchange sets the Dead Letter Exchange name.
//
// Default: "dlx.exchange"
func WithDLQExchange(exchangeName string) Option {
	return func(c *Config) {
		c.DLQConfig.ExchangeName = exchangeName
	}
}

// WithDLQPrefix sets the prefix for DLQ queue names.
//
// Default: "dlq."
func WithDLQPrefix(prefix string) Option {
	return func(c *Config) {
		c.DLQConfig.QueuePrefix = prefix
	}
}
