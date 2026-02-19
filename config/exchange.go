package config

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
	Args map[string]any
}
