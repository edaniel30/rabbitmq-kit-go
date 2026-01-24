# Configuration Guide

Complete reference for configuring rabbitmq-kit-go.

## Table of Contents

- [Default Configuration](#default-configuration)
- [Connection Options](#connection-options)
- [Publisher Options](#publisher-options)
- [Consumer Options](#consumer-options)
- [Topology Setup](#topology-setup)
- [Circuit Breaker](#circuit-breaker)
- [Dead Letter Queue](#dead-letter-queue)
- [Common Patterns](#common-patterns)

## Default Configuration

```go
config.DefaultConfig()
// Returns:
// - ReconnectDelay: 5s
// - Timeout: 10s
// - PrefetchCount: 10
// - MaxRetries: 3
// - PublisherConfirms: false
// - ConfirmTimeout: 5s
// - Logger: DefaultLogger (stdout)
```

## Connection Options

### WithURI

Set RabbitMQ connection URI:

```go
config.WithURI("amqp://user:pass@localhost:5672/vhost")
```

**Format**: `amqp://username:password@host:port/vhost`

### WithReconnectDelay

Set delay between reconnection attempts:

```go
config.WithReconnectDelay(10 * time.Second)
```

**Recommendation**: 5-10s for production.

### WithTimeout

Set default timeout for operations:

```go
config.WithTimeout(15 * time.Second)
```

**Used for**: Publish operations without explicit context timeout.

## Publisher Options

### WithPublisherConfirms

Enable guaranteed delivery with publisher confirms:

```go
config.WithPublisherConfirms(true)
```

**When enabled**:
- `Publish()` waits for ACK/NACK from broker
- Returns `ErrPublishNotConfirmed` on NACK
- Returns `ErrPublishConfirmTimeout` on timeout

**Recommendation**: Always enable in production for critical messages.

### WithConfirmTimeout

Set timeout for publisher confirms:

```go
config.WithConfirmTimeout(5 * time.Second)
```

**Default**: 5s

## Consumer Options

### WithPrefetchCount

Set number of unacknowledged messages per consumer:

```go
config.WithPrefetchCount(20)
```

**Guidelines**:
- Fast processing (<100ms): 50-100
- Medium (100ms-1s): 10-20
- Slow (>1s): 1-5

**Important**: Higher prefetch = better throughput but more memory usage.

### WithMaxRetries

Set maximum retry attempts for failed messages:

```go
config.WithMaxRetries(5)
```

**Behavior**:
- Handler returns error → retry count incremented
- If count >= MaxRetries → message sent to DLQ (if configured) or discarded

**Default**: 3

## Topology Setup

### Exchanges

Declare exchanges automatically on connection:

```go
config.WithExchanges([]config.ExchangeConfig{
    {
        Name:       "users.exchange",
        Type:       "topic",        // topic, direct, fanout, headers
        Durable:    true,
        AutoDelete: false,
        Internal:   false,
        NoWait:     false,
        Args:       nil,
    },
})
```

**Exchange Types**:
- `topic`: Pattern matching (e.g., `user.created`, `user.*`)
- `direct`: Exact routing key match
- `fanout`: Broadcast to all queues
- `headers`: Match on message headers

### Queues

Declare and bind queues automatically:

```go
config.WithQueues([]config.QueueConfig{
    {
        Name:        "users.queue",
        Exchange:    "users.exchange",
        RoutingKeys: []string{"user.created", "user.updated"},
        Durable:     true,
        AutoDelete:  false,
        Exclusive:   false,
        NoWait:      false,
        Args:        nil,
    },
})
```

**Important**: The library automatically binds the queue to the exchange with specified routing keys.

### Queue Arguments

Common queue arguments:

```go
Args: map[string]any{
    // Dead Letter Exchange
    "x-dead-letter-exchange":    "my.dlx",
    "x-dead-letter-routing-key": "failed.messages",

    // Message TTL (milliseconds)
    "x-message-ttl": int32(3600000), // 1 hour

    // Queue Length Limit
    "x-max-length": int32(10000),

    // Queue TTL (when unused)
    "x-expires": int32(86400000), // 24 hours
}
```

## Circuit Breaker

Enable circuit breaker to protect consumers from cascading failures:

```go
config.WithCircuitBreaker(
    true,              // enabled
    5,                 // maxFailures
    10*time.Second,    // resetTimeout
    3,                 // halfOpenRequests
)
```

**States**:
1. **CLOSED** (normal): All requests allowed
2. **OPEN** (failing): All requests rejected after maxFailures
3. **HALF-OPEN** (testing): Allow halfOpenRequests test requests after resetTimeout

See [Circuit Breaker Guide](CIRCUIT_BREAKER.md) for details.

## Dead Letter Queue

### Auto-setup DLQ

Automatically create DLQ infrastructure:

```go
config.WithDLQEnabled(
    true,    // enabled
    "dlq.",  // prefix for DLQ names
)
```

**Creates**:
- DLX exchange: `dlq.exchange`
- DLQ for each queue: `dlq.{queue_name}`
- Bindings with routing key: `{queue_name}`

### Manual DLQ Setup

For custom DLQ configuration, declare manually:

```go
config.WithExchanges([]config.ExchangeConfig{
    {Name: "orders.exchange", Type: "topic", Durable: true},
    {Name: "orders.dlx", Type: "topic", Durable: true},
})

config.WithQueues([]config.QueueConfig{
    {
        Name:        "orders.queue",
        Exchange:    "orders.exchange",
        RoutingKeys: []string{"order.#"},
        Durable:     true,
        Args: map[string]any{
            "x-dead-letter-exchange":    "orders.dlx",
            "x-dead-letter-routing-key": "failed.orders",
        },
    },
    {
        Name:        "orders.dlq",
        Exchange:    "orders.dlx",
        RoutingKeys: []string{"failed.orders"},
        Durable:     true,
    },
})
```

See [DLQ Guide](DLQ.md) for handling DLQ messages.

## Common Patterns

### Development Configuration

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
    config.WithPrefetchCount(10),
    config.WithMaxRetries(3),
)
```

### Production Configuration

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://prod-user:password@rabbitmq.prod:5672/prod-vhost"),
    config.WithPublisherConfirms(true),
    config.WithConfirmTimeout(5 * time.Second),
    config.WithPrefetchCount(20),
    config.WithMaxRetries(5),
    config.WithCircuitBreaker(true, 10, 30*time.Second, 5),
    config.WithDLQEnabled(true, "dlq."),
    config.WithReconnectDelay(10 * time.Second),
    config.WithTimeout(15 * time.Second),
)
```

### High-Throughput Configuration

For maximum throughput with fast message processing:

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://user:pass@host:5672/"),
    config.WithPublisherConfirms(true),
    config.WithPrefetchCount(100),    // High prefetch
    config.WithMaxRetries(1),          // Fail fast
)

// Start many workers
eventBus.StartConsume("queue", 20)
```

### Low-Latency Configuration

For critical, low-latency operations:

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://user:pass@host:5672/"),
    config.WithPublisherConfirms(true),
    config.WithConfirmTimeout(1 * time.Second),  // Fast timeout
    config.WithPrefetchCount(1),                  // Low prefetch
    config.WithTimeout(2 * time.Second),          // Fast operations
)
```

### With Custom Logger

Use your own logger implementation:

```go
type MyLogger struct {
    logger *zap.Logger
}

func (l *MyLogger) Info(ctx context.Context, msg string, fields map[string]any) {
    l.logger.Info(msg, zap.Any("fields", fields))
}
// ... implement Error, Warn, Debug, Close

eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://..."),
    config.WithLogger(myLogger),
)
```

## Configuration Validation

The library validates configuration on `NewEventBus()`:

```go
eventBus, err := rabbitmq.NewEventBus(config.DefaultConfig())
// Returns: ConfigError{Field: "URI", Message: "is required"}
```

**Validated fields**:
- `URI` must not be empty
- `ReconnectDelay` must be > 0
- `Timeout` must be > 0
- `PrefetchCount` must be > 0
- `ConfirmTimeout` must be > 0 (if PublisherConfirms enabled)
