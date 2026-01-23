package config

// DLQConfig holds the configuration for Dead Letter Queue (DLQ) setup.
//
// When enabled, the library will automatically:
//   - Create a Dead Letter Exchange (DLX)
//   - Create a Dead Letter Queue (DLQ) with naming pattern "dlq.{queueName}"
//   - Configure main queues with x-dead-letter-exchange and x-dead-letter-routing-key
//   - Route failed messages (after max retries) to DLQ for analysis
//
// Example:
//
//	cfg := config.DefaultConfig()
//	cfg.DLQEnabled = true
//	cfg.DLQConfig = config.DLQConfig{
//	    ExchangeName: "my.dlx",
//	    QueuePrefix:  "dlq.",
//	}
type DLQConfig struct {
	// ExchangeName is the name of the Dead Letter Exchange.
	// Default: "dlx.exchange"
	ExchangeName string

	// QueuePrefix is the prefix to use for DLQ queue names.
	// The full DLQ name will be: QueuePrefix + original queue name
	// Default: "dlq."
	QueuePrefix string

	// ExchangeType is the type of the Dead Letter Exchange.
	// Default: "direct"
	ExchangeType string

	// Durable specifies if the DLX and DLQ should be durable.
	// Default: true
	Durable bool

	// AutoDelete specifies if the DLX and DLQ should be auto-deleted.
	// Default: false
	AutoDelete bool
}

// DefaultDLQConfig returns a DLQConfig with sensible defaults.
func DefaultDLQConfig() DLQConfig {
	return DLQConfig{
		ExchangeName: "dlx.exchange",
		QueuePrefix:  "dlq.",
		ExchangeType: "direct",
		Durable:      true,
		AutoDelete:   false,
	}
}

// GetDLQName returns the DLQ queue name for a given queue.
//
// Example:
//
//	dlqConfig := config.DefaultDLQConfig()
//	dlqName := dlqConfig.GetDLQName("orders.queue")
//	// Returns: "dlq.orders.queue"
func (c *DLQConfig) GetDLQName(queueName string) string {
	return c.QueuePrefix + queueName
}

// GetDLXRoutingKey returns the routing key for DLX bindings.
//
// By default, uses the original queue name as the routing key.
func (c *DLQConfig) GetDLXRoutingKey(queueName string) string {
	return queueName
}
