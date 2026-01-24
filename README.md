# rabbitmq-kit-go

A production-ready RabbitMQ library for Go with high-level abstractions for event-driven architectures following Domain-Driven Design patterns.

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue.svg)](https://golang.org)
[![Coverage](https://img.shields.io/badge/coverage-81%25-green.svg)](https://github.com/edaniel30/rabbitmq-kit-go)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Installation

```bash
go get github.com/edaniel30/rabbitmq-kit-go
```

## Quick Start

### Publisher

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

    // Publish event
    event := &UserCreatedEvent{
        UserID: "user-123",
        Email:  "user@example.com",
    }

    if err := eventBus.Publish(context.Background(), event); err != nil {
        log.Printf("Failed to publish: %v", err)
    }
}
```

### Consumer

```go
package main

import (
    "log"

    "github.com/edaniel30/rabbitmq-kit-go"
    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/router"
)

func main() {
    eventBus, _ := rabbitmq.NewEventBus(
        config.DefaultConfig(),
        config.WithURI("amqp://guest:guest@localhost:5672/"),
    )
    defer eventBus.Close()

    // Register handlers
    eventBus.RegisterHandler("user.created", func(ctx *router.MessageContext) error {
        var user map[string]any
        if err := ctx.BindJSON(&user); err != nil {
            return err // Will retry
        }
        log.Printf("User created: %+v", user)
        return nil // Will ACK
    })

    // Start consuming with 5 workers
    eventBus.StartConsume("users.queue", 5)
    select {} // Keep running
}
```

For complete examples, see [examples/](examples/).

## Documentation

### 📖 [Configuration Guide](docs/CONFIGURATION.md)
All configuration options, topology setup, and common patterns.

### ⚡ [Circuit Breaker](docs/CIRCUIT_BREAKER.md)
Protect your services from cascading failures with automatic circuit breaking.

### 💀 [Dead Letter Queue (DLQ)](docs/DLQ.md)
Handle failed messages, requeue from DLQ, and monitor poison messages.

### 🚀 [Batch Publishing](docs/BATCH_PUBLISHING.md)
Pipelined batch publishing and async worker pools for maximum throughput.

## Key Concepts

### Event Interface

Events must implement the `Event` interface:

```go
type UserCreatedEvent struct {
    UserID string `json:"user_id"`
    Email  string `json:"email"`
}

func (e *UserCreatedEvent) Type() string      { return "user.created" }
func (e *UserCreatedEvent) Exchange() string  { return "users.exchange" }
func (e *UserCreatedEvent) ToMap() map[string]any {
    return map[string]any{
        "type":    e.Type(),
        "user_id": e.UserID,
        "email":   e.Email,
    }
}
```

### Handler Return Values

```go
eventBus.RegisterHandler("user.created", func(ctx *router.MessageContext) error {
    // return nil      → ACK (message processed)
    // return error    → NACK with retry (uses x-retry-count)
    //                   After MaxRetries → NACK without requeue (goes to DLQ)
})
```

### Publisher Confirms

```go
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithPublisherConfirms(true),
    config.WithConfirmTimeout(5 * time.Second),
)

// Publish waits for confirmation
err := eventBus.Publish(ctx, event)
// Returns ErrPublishNotConfirmed on NACK
// Returns ErrPublishConfirmTimeout on timeout
```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithURI` | **(required)** | RabbitMQ connection URI |
| `WithPublisherConfirms` | `false` | Enable guaranteed delivery |
| `WithConfirmTimeout` | `5s` | Timeout for confirmations |
| `WithMaxRetries` | `3` | Max retry attempts |
| `WithPrefetchCount` | `10` | Unacked messages per worker |
| `WithCircuitBreaker` | `false` | Enable circuit breaker |
| `WithDLQEnabled` | `false` | Auto-setup DLQ infrastructure |

See [Configuration Guide](docs/CONFIGURATION.md) for all options.

## Batch Publishing

### Pipelined Batch (5-10x faster)

```go
events := []Event{event1, event2, event3}

// Pipelining: send all first, wait for all confirms
result, err := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(true),
)
```

### Async Batch with Worker Pool

```go
// Concurrent publishing with limited workers
result, err := eventBus.PublishBatchAsync(ctx, events,
    config.WithMaxConcurrency(50),
)
```

See [Batch Publishing Guide](docs/BATCH_PUBLISHING.md) for performance comparisons.

## Circuit Breaker

Protect consumers from cascading failures:

```go
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithCircuitBreaker(true, 5, 10*time.Second, 3),
)

// After 5 failures → Circuit OPEN (reject all)
// After 10s → Circuit HALF-OPEN (allow 3 test requests)
// After 3 successes → Circuit CLOSED (normal operation)
```

See [Circuit Breaker Guide](docs/CIRCUIT_BREAKER.md).

## Dead Letter Queue (DLQ)

### Auto-setup DLQ

```go
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithDLQEnabled(true, "dlq."),
)

// Automatically creates:
// - dlq.exchange (DLX)
// - dlq.{queue_name} for each queue
// - Bindings with proper routing
```

### Manual DLQ Handler

```go
eventBus.RegisterDLQHandler("user.created", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    log.Printf("Failed after %d retries: %s",
        dlqMsg.RetryCount,
        dlqMsg.GetDeathInfo(),
    )

    // Requeue if appropriate
    if dlqMsg.ShouldRetry(10) {
        return eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
    }

    return nil // Discard
})

eventBus.StartConsumeDLQ("dlq.users.queue", 1)
```

See [DLQ Guide](docs/DLQ.md).

## Error Handling

```go
import "github.com/edaniel30/rabbitmq-kit-go/errors"

// Sentinel errors
errors.ErrPublishNotConfirmed   // NACK from broker
errors.ErrPublishConfirmTimeout // Confirmation timeout
errors.ErrMaxRetriesExceeded    // Exceeded max retries
errors.ErrClientClosed          // Client closed

// Typed errors
&errors.PublishError{Exchange: "users", RoutingKey: "user.created", Cause: err}
&errors.ConsumeError{Queue: "users.queue", Cause: err}
&errors.TopologyError{Operation: "declare_exchange", Resource: "users", Cause: err}
```

## Production Best Practices

1. **Always enable Publisher Confirms** for critical messages
2. **Configure DLQ** for failed message handling
3. **Set appropriate prefetch**:
   - Fast processing (<100ms): 50-100
   - Medium (100ms-1s): 10-20
   - Slow (>1s): 1-5
4. **Use Circuit Breaker** to prevent cascading failures
5. **Monitor DLQ** for poison messages
6. **Use context timeouts** for all operations
7. **Graceful shutdown**: `defer eventBus.Close()`

## Requirements

- Go 1.21+
- RabbitMQ 3.8+

## Contributing

We welcome contributions! Please ensure:
- All tests pass (`make test`)
- Coverage meets 80% threshold (`make test-coverage`)
- Code follows Go best practices

## License

MIT License - see [LICENSE](LICENSE) file for details.