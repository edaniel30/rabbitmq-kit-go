package config

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

// WithDLX configures a queue to use Dead Letter Exchange.
//
// This helper automatically sets the x-dead-letter-exchange and
// x-dead-letter-routing-key arguments for the queue.
//
// The method is idempotent - if DLX is already configured, it won't overwrite.
//
// Example:
//
//	queue := config.QueueConfig{
//	    Name:    "orders.queue",
//	    Durable: true,
//	}
//	queue.WithDLX("dlx.exchange", "orders.queue")
func (q *QueueConfig) WithDLX(dlxExchange, routingKey string) *QueueConfig {
	if q.Args == nil {
		q.Args = make(map[string]any)
	}
	// Idempotent - only set if not already configured
	if _, exists := q.Args["x-dead-letter-exchange"]; !exists {
		q.Args["x-dead-letter-exchange"] = dlxExchange
		q.Args["x-dead-letter-routing-key"] = routingKey
	}
	return q
}

// CreateDLQQueue creates a DLQ queue configuration for a given main queue.
//
// The DLQ queue will have the same durability settings as the main queue.
//
// Example:
//
//	dlqConfig := config.DefaultDLQConfig()
//	mainQueue := config.QueueConfig{Name: "orders.queue", Durable: true}
//	dlqQueue := config.CreateDLQQueue(mainQueue, dlqConfig)
//	// Returns: QueueConfig{Name: "dlq.orders.queue", Durable: true, ...}
func CreateDLQQueue(mainQueue QueueConfig, dlqConfig DLQConfig) QueueConfig {
	return QueueConfig{
		Name:        dlqConfig.GetDLQName(mainQueue.Name),
		Exchange:    dlqConfig.ExchangeName,
		RoutingKeys: []string{dlqConfig.GetDLXRoutingKey(mainQueue.Name)},
		Durable:     dlqConfig.Durable,
		AutoDelete:  dlqConfig.AutoDelete,
		Exclusive:   false,
		Args:        nil,
	}
}
