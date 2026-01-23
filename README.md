# RabbitMQ Kit Go

A production-ready RabbitMQ library for Go that provides high-level abstractions for event-driven architectures following Domain-Driven Design patterns.

## Features

- 🔌 **Auto-reconnection**: Automatic reconnection with configurable backoff
- 🏗️ **Topology Management**: Automatic creation of exchanges, queues, and bindings
- 📨 **Event Bus Pattern**: High-level Event Bus for Domain-Driven Design
- ✅ **Publisher Confirms**: Guaranteed message delivery with broker acknowledgments
- 🔁 **Retry Mechanism**: Automatic retry with exponential backoff using x-retry-count headers
- 💀 **Dead Letter Exchange**: Built-in DLX support for failed messages
- 👷 **Worker Pools**: Multi-worker message consumption with configurable concurrency
- 🎯 **Event Routing**: Type-based message routing with handler registration
- ⚙️ **Functional Options**: Clean, idiomatic configuration pattern
- 🔒 **Thread-Safe**: Concurrent-safe operations with proper locking
- 🎯 **Context Support**: Full context.Context support for timeouts and cancellation

## Installation

```bash
go get github.com/edaniel30/rabbitmq-kit-go
```

## Quick Start

### Publisher Example

```go
package main

import (
    "context"
    "log"

    "github.com/edaniel30/rabbitmq-kit-go"
    "github.com/edaniel30/rabbitmq-kit-go/config"
)

func main() {
    // Create EventBus
    eventBus, err := rabbitmq.NewEventBus(
        config.DefaultConfig(),
        config.WithURI("amqp://guest:guest@localhost:5672/"),
        config.WithPublisherConfirms(true),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer eventBus.Close()

    // Publish an event
    event := &UserCreatedEvent{
        UserID: "user-123",
        Email:  "user@example.com",
    }

    ctx := context.Background()
    if err := eventBus.Publish(ctx, event); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}
```

### Consumer Example

```go
package main

import (
    "log"

    "github.com/edaniel30/rabbitmq-kit-go"
    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/router"
)

func main() {
    // Create EventBus
    eventBus, err := rabbitmq.NewEventBus(
        config.DefaultConfig(),
        config.WithURI("amqp://guest:guest@localhost:5672/"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer eventBus.Close()

    // Register handlers
    eventBus.RegisterHandler("user.created", handleUserCreated)
    eventBus.RegisterHandler("user.updated", handleUserUpdated)

    // Start consuming with 5 workers
    if err := eventBus.StartConsume("users.queue", 5); err != nil {
        log.Fatal(err)
    }

    // Keep application running
    select {}
}

func handleUserCreated(ctx *router.MessageContext) error {
    var data map[string]interface{}
    if err := ctx.BindJSON(&data); err != nil {
        return err
    }

    log.Printf("User created: %+v", data)
    return nil
}

func handleUserUpdated(ctx *router.MessageContext) error {
    var data map[string]interface{}
    if err := ctx.BindJSON(&data); err != nil {
        return err
    }

    log.Printf("User updated: %+v", data)
    return nil
}
```

## Configuration

### Basic Configuration

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
    config.WithReconnectDelay(10 * time.Second),
    config.WithTimeout(15 * time.Second),
    config.WithPrefetchCount(20),
    config.WithMaxRetries(5),
)
```

### Configuration with Topology

Automatically create exchanges, queues, and bindings:

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
    config.WithExchanges([]config.ExchangeConfig{
        {
            Name:       "users.exchange",
            Type:       "topic",
            Durable:    true,
            AutoDelete: false,
        },
        {
            Name:       "orders.exchange",
            Type:       "direct",
            Durable:    true,
            AutoDelete: false,
        },
    }),
    config.WithQueues([]config.QueueConfig{
        {
            Name:        "users.queue",
            Exchange:    "users.exchange",
            RoutingKeys: []string{"user.created", "user.updated", "user.deleted"},
            Durable:     true,
            AutoDelete:  false,
        },
        {
            Name:        "orders.queue",
            Exchange:    "orders.exchange",
            RoutingKeys: []string{"order.created", "order.completed"},
            Durable:     true,
            AutoDelete:  false,
        },
    }),
)
```

### Dead Letter Exchange (DLX) Configuration

Configure queues with DLX for handling failed messages:

```go
config.WithQueues([]config.QueueConfig{
    {
        Name:        "orders.queue",
        Exchange:    "orders.exchange",
        RoutingKeys: []string{"order.created"},
        Durable:     true,
        Args: map[string]any{
            "x-dead-letter-exchange":    "orders.dlx",
            "x-dead-letter-routing-key": "failed.orders",
            "x-message-ttl":             int32(3600000), // 1 hour in milliseconds
        },
    },
})
```

### Publisher Confirms

Enable publisher confirms for guaranteed delivery (recommended for production):

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
    config.WithPublisherConfirms(true),
    config.WithConfirmTimeout(5 * time.Second),
)
```

When enabled, the `Publish()` method will:
- Wait for RabbitMQ to confirm message receipt
- Return `errors.ErrPublishNotConfirmed` if the message is rejected (NACK)
- Return `errors.ErrPublishConfirmTimeout` if confirmation times out
- Ensure messages are persisted before returning

## Configuration Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `WithURI` | `string` | **(required)** | RabbitMQ connection URI |
| `WithReconnectDelay` | `time.Duration` | `5s` | Delay between reconnection attempts |
| `WithTimeout` | `time.Duration` | `10s` | Default timeout for operations |
| `WithPrefetchCount` | `int` | `10` | Number of unacknowledged messages per consumer |
| `WithMaxRetries` | `int` | `3` | Maximum retries for failed messages |
| `WithPublisherConfirms` | `bool` | `false` | Enable publisher confirms |
| `WithConfirmTimeout` | `time.Duration` | `5s` | Timeout for publisher confirms |
| `WithExchanges` | `[]ExchangeConfig` | `[]` | Exchanges to declare on connect |
| `WithQueues` | `[]QueueConfig` | `[]` | Queues to declare and bind on connect |

## Event Interface

To use the EventBus, your events must implement the `Event` interface:

```go
type Event interface {
    // Type returns the event type identifier (e.g., "user.created", "order.completed")
    Type() string

    // Exchange returns the exchange name where this event should be published
    Exchange() string

    // ToMap converts the event to a map for JSON serialization
    ToMap() map[string]interface{}
}
```

### Example Event Implementation

```go
type UserCreatedEvent struct {
    UserID    string    `json:"user_id"`
    Email     string    `json:"email"`
    CreatedAt time.Time `json:"created_at"`
}

func (e *UserCreatedEvent) Type() string {
    return "user.created"
}

func (e *UserCreatedEvent) Exchange() string {
    return "users.exchange"
}

func (e *UserCreatedEvent) ToMap() map[string]interface{} {
    return map[string]interface{}{
        "type":       e.Type(),
        "user_id":    e.UserID,
        "email":      e.Email,
        "created_at": e.CreatedAt,
    }
}
```

**Important**: The `ToMap()` method must include a `"type"` field for the router to work correctly.

## Publishing Events

### Single Event

```go
event := &OrderCreatedEvent{
    OrderID:   "order-123",
    UserID:    "user-456",
    Total:     99.99,
    CreatedAt: time.Now(),
}

ctx := context.Background()
err := eventBus.Publish(ctx, event)
```

### Batch Publishing (Sequential)

Useful for publishing domain events accumulated in aggregates:

```go
// In your domain aggregate
order.AddProduct(productID, quantity)
order.RemoveProduct(otherProductID)

// In your application layer (after successful DB commit)
events := order.PullEvents() // Returns []Event

ctx := context.Background()
err := eventBus.PublishBatch(ctx, events)
```

Events are published sequentially. If any event fails, the method stops and returns the error.

### Batch Publishing (Concurrent)

For large batches where order doesn't matter:

```go
events := []rabbitmq.Event{event1, event2, event3}
err := eventBus.PublishBatchAsync(ctx, events)
```

**Warning**: Events may be published out of order. Use only when event ordering is not important.

## Consuming Events

### Register Handlers

```go
eventBus.RegisterHandler("user.created", func(ctx *router.MessageContext) error {
    var user User
    if err := ctx.BindJSON(&user); err != nil {
        return err // Will trigger retry
    }

    log.Printf("Processing user: %+v", user)

    // Your business logic here
    if err := saveToDatabase(user); err != nil {
        return err // Will trigger retry
    }

    return nil // Message will be acknowledged
})
```

### Start Consuming

```go
// Start consuming with 5 concurrent workers
err := eventBus.StartConsume("users.queue", 5)
if err != nil {
    log.Fatal(err)
}
```

### Handler Return Values

The handler's return value determines message acknowledgment:

- `return nil`: Message is **acknowledged** (ACK)
- `return error`: Message enters **retry logic** based on `x-retry-count` header:
  - If retries < `MaxRetries`: Message is rejected with `requeue=true` (NACK with requeue)
  - If retries >= `MaxRetries`: Message is rejected with `requeue=false` (sent to DLX if configured)

### Accessing Message Context

```go
func handler(ctx *router.MessageContext) error {
    // Get event type
    eventType := ctx.GetType() // e.g., "user.created"

    // Get headers
    retryCount := ctx.GetHeader("x-retry-count")

    // Bind JSON body
    var payload map[string]interface{}
    if err := ctx.BindJSON(&payload); err != nil {
        return err
    }

    // Your processing logic
    return nil
}
```

## Retry Mechanism

The library automatically handles retries using the `x-retry-count` header:

1. Handler returns an error
2. Library checks `x-retry-count` header (default 0)
3. If retry count < `MaxRetries`:
   - Increments `x-retry-count`
   - Rejects message with `requeue=true` (NACK)
   - RabbitMQ redelivers the message
4. If retry count >= `MaxRetries`:
   - Logs error with `ErrMaxRetriesExceeded`
   - Rejects message with `requeue=false` (NACK)
   - Message goes to DLX if configured, otherwise discarded

### Configuring Max Retries

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
    config.WithMaxRetries(5), // Retry up to 5 times
)
```

## Error Handling

### Sentinel Errors

```go
import "github.com/edaniel30/rabbitmq-kit-go/errors"

// Client errors
errors.ErrClientClosed          // Client is closed
errors.ErrNoConnection          // No active connection
errors.ErrNoChannel             // No active channel
errors.ErrNoHandler             // No handler registered for event type

// Publishing errors
errors.ErrPublishNotConfirmed   // Message not confirmed (NACK)
errors.ErrPublishConfirmTimeout // Confirmation timeout

// Consumer errors
errors.ErrMaxRetriesExceeded       // Message exceeded max retries
errors.ErrNoHandlersRegistered     // StartConsume called without handlers
```

### Typed Errors

```go
import "github.com/edaniel30/rabbitmq-kit-go/errors"

// Configuration error
&errors.ConfigError{Field: "URI", Message: "is required"}

// Connection error
&errors.ConnectionError{Operation: "dial", Cause: err}

// Publish error
&errors.PublishError{Exchange: "users", RoutingKey: "user.created", Cause: err}

// Consume error
&errors.ConsumeError{Queue: "users.queue", Cause: err}

// Topology error
&errors.TopologyError{Operation: "declare_exchange", Resource: "users.exchange", Cause: err}

// Handler error
&errors.HandlerError{EventType: "user.created", MessageID: "msg-123", Cause: err}
```

### Error Handling Example

```go
if err := eventBus.Publish(ctx, event); err != nil {
    switch {
    case errors.Is(err, errors.ErrPublishNotConfirmed):
        log.Printf("Message rejected by broker (NACK)")
    case errors.Is(err, errors.ErrPublishConfirmTimeout):
        log.Printf("Confirmation timeout - message may or may not be delivered")
    case errors.Is(err, errors.ErrClientClosed):
        log.Printf("Client is closed, reconnection in progress")
    default:
        var pubErr *errors.PublishError
        if errors.As(err, &pubErr) {
            log.Printf("Publish failed [exchange=%s, key=%s]: %v",
                pubErr.Exchange, pubErr.RoutingKey, pubErr.Cause)
        }
    }
}
```

## Advanced Usage

### Custom Publishing Options

For full control over message properties:

```go
import (
    "github.com/edaniel30/rabbitmq-kit-go/internal/broker"
    amqp "github.com/rabbitmq/amqp091-go"
)

// Get publisher from EventBus (note: this requires accessing internal fields)
// For production use, consider extending the EventBus API

publisher := broker.NewPublisher(client)
err := publisher.PublishWithOptions(ctx, "exchange", "routing.key", amqp.Publishing{
    ContentType:  "application/json",
    Body:         []byte(`{"data":"value"}`),
    DeliveryMode: amqp.Persistent,
    Priority:     5,
    Timestamp:    time.Now(),
    Headers: amqp.Table{
        "x-custom-header": "value",
        "x-retry-count":   0,
    },
})
```

### Context Management

The EventBus client provides helper methods for context management:

```go
// Create context with default timeout (from config)
ctx, cancel := client.NewContext()
defer cancel()

// Add timeout to existing context
ctx, cancel := client.WithTimeout(parentCtx)
defer cancel()

// Ensure context has a deadline (adds timeout only if needed)
ctx, cancel := client.EnsureTimeout(ctx)
defer cancel()
```

## Complete Example

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/edaniel30/rabbitmq-kit-go"
    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/router"
)

// Domain Event
type OrderCreatedEvent struct {
    OrderID   string    `json:"order_id"`
    UserID    string    `json:"user_id"`
    Total     float64   `json:"total"`
    CreatedAt time.Time `json:"created_at"`
}

func (e *OrderCreatedEvent) Type() string { return "order.created" }
func (e *OrderCreatedEvent) Exchange() string { return "orders.exchange" }
func (e *OrderCreatedEvent) ToMap() map[string]interface{} {
    return map[string]interface{}{
        "type":       e.Type(),
        "order_id":   e.OrderID,
        "user_id":    e.UserID,
        "total":      e.Total,
        "created_at": e.CreatedAt,
    }
}

func main() {
    // Create EventBus with full configuration
    eventBus, err := rabbitmq.NewEventBus(
        config.DefaultConfig(),
        config.WithURI("amqp://guest:guest@localhost:5672/"),
        config.WithPrefetchCount(10),
        config.WithMaxRetries(3),
        config.WithPublisherConfirms(true),
        config.WithExchanges([]config.ExchangeConfig{
            {
                Name:    "orders.exchange",
                Type:    "topic",
                Durable: true,
            },
            {
                Name:    "orders.dlx",
                Type:    "topic",
                Durable: true,
            },
        }),
        config.WithQueues([]config.QueueConfig{
            {
                Name:        "orders.queue",
                Exchange:    "orders.exchange",
                RoutingKeys: []string{"order.created", "order.completed"},
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
        }),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer eventBus.Close()

    // Register handlers
    eventBus.RegisterHandler("order.created", handleOrderCreated)
    eventBus.RegisterHandler("order.completed", handleOrderCompleted)

    // Start consumer with 5 workers
    go func() {
        if err := eventBus.StartConsume("orders.queue", 5); err != nil {
            log.Printf("Consumer error: %v", err)
        }
    }()

    // Publish an event
    event := &OrderCreatedEvent{
        OrderID:   "order-123",
        UserID:    "user-456",
        Total:     199.99,
        CreatedAt: time.Now(),
    }

    ctx := context.Background()
    if err := eventBus.Publish(ctx, event); err != nil {
        log.Printf("Publish error: %v", err)
    }

    // Keep application running
    select {}
}

func handleOrderCreated(ctx *router.MessageContext) error {
    var order OrderCreatedEvent
    if err := ctx.BindJSON(&order); err != nil {
        return err
    }

    log.Printf("Processing order: %+v", order)

    // Business logic here
    // If this returns an error, the message will be retried

    return nil
}

func handleOrderCompleted(ctx *router.MessageContext) error {
    var data map[string]interface{}
    if err := ctx.BindJSON(&data); err != nil {
        return err
    }

    log.Printf("Order completed: %+v", data)
    return nil
}
```

## Production Best Practices

1. **Always enable Publisher Confirms** for critical messages:
   ```go
   config.WithPublisherConfirms(true)
   ```

2. **Configure Dead Letter Exchange** for failed messages:
   ```go
   Args: map[string]any{
       "x-dead-letter-exchange": "my.dlx",
   }
   ```

3. **Set appropriate prefetch count** based on message processing time:
   - Fast processing (<100ms): 50-100
   - Medium processing (100ms-1s): 10-20
   - Slow processing (>1s): 1-5

4. **Use context with timeout** for all publish operations:
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
   defer cancel()
   ```

5. **Monitor retry counts** and adjust `MaxRetries` based on your needs

6. **Log all errors** with structured logging for debugging

7. **Graceful shutdown**:
   ```go
   defer eventBus.Close()
   ```

## Requirements

- Go 1.21 or higher
- RabbitMQ 3.8 or higher

## License

MIT

## Contributing

Contributions are welcome! Please open an issue or pull request.

## Related Libraries

This library follows the same architectural patterns as:
- [loki-logger-go](https://github.com/edaniel30/loki-logger-go)
- [http-platform-go](https://github.com/edaniel30/http-platform-go)
- [mongo-kit-go](https://github.com/edaniel30/mongo-kit-go)
