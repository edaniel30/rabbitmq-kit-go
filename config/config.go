package config

import (
	"fmt"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
)

// ExchangeConfig defines configuration for a RabbitMQ exchange.
type ExchangeConfig struct {
	// Name is the name of the exchange
	Name string

	// Type is the exchange type: "direct", "fanout", "topic", or "headers"
	Type string

	// Durable indicates if the exchange should survive broker restart
	// Default: true
	Durable bool

	// AutoDelete indicates if the exchange should be deleted when no longer used
	// Default: false
	AutoDelete bool

	// Internal indicates if the exchange is internal (cannot be published to directly)
	// Default: false
	Internal bool

	// Args are optional exchange arguments
	Args map[string]interface{}
}

// QueueConfig defines configuration for a RabbitMQ queue with bindings.
type QueueConfig struct {
	// Name is the name of the queue
	Name string

	// Exchange is the name of the exchange to bind this queue to (optional)
	Exchange string

	// RoutingKeys are the routing keys to use for binding (optional)
	// Multiple routing keys will create multiple bindings
	RoutingKeys []string

	// Durable indicates if the queue should survive broker restart
	// Default: true
	Durable bool

	// AutoDelete indicates if the queue should be deleted when no longer used
	// Default: false
	AutoDelete bool

	// Exclusive indicates if the queue is exclusive to this connection
	// Default: false
	Exclusive bool

	// Args are optional queue arguments for advanced features.
	// Common arguments:
	//   - "x-dead-letter-exchange": DLX name for failed messages
	//   - "x-dead-letter-routing-key": routing key for DLX
	//   - "x-message-ttl": message TTL in milliseconds (int32)
	//   - "x-max-length": maximum number of messages in queue (int32)
	//   - "x-max-priority": maximum priority level (int32, typically 1-10)
	Args map[string]any
}

// Config holds the configuration for the RabbitMQ client.
type Config struct {
	// Required fields
	URI string // RabbitMQ connection URI (e.g., "amqp://user:pass@host:5672/")

	// Optional fields with defaults
	ReconnectDelay time.Duration // Delay between reconnection attempts (default: 5s)
	Timeout        time.Duration // Default timeout for operations (default: 10s)
	PrefetchCount  int           // Number of messages to prefetch (default: 10)
	MaxRetries     int           // Maximum number of retries for failed messages (default: 3)

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
//   - Exchanges: empty
//   - Queues: empty
func DefaultConfig() Config {
	return Config{
		ReconnectDelay: 5 * time.Second,
		Timeout:        10 * time.Second,
		PrefetchCount:  10,
		MaxRetries:     3,
		Exchanges:      []ExchangeConfig{},
		Queues:         []QueueConfig{},
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
