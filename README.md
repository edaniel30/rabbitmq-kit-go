# RabbitMQ Kit Go

A high-level RabbitMQ library for Go that simplifies connection management, message publishing, consuming, and event routing.

## Features

- 🔌 **Auto-reconnection**: Automatic reconnection handling on connection loss
- 🏗️ **Auto-topology setup**: Automatically creates exchanges, queues, and bindings
- 📨 **Simple publishing**: Easy message publishing with context support
- 👷 **Worker support**: Multi-worker message consumption
- 🎯 **Event routing**: Route messages by event type using a router
- ⚙️ **Functional options**: Clean configuration with functional options pattern

## Installation

```bash
go get github.com/edaniel30/rabbitmq-kit-go
```

## Quick Start

### Basic Setup

```go
package main

import (
    "context"
    "log"
    
    "github.com/edaniel30/rabbitmq-kit-go"
)

func main() {
    // Create a new broker
    broker := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/")
    
    // Connect to RabbitMQ
    if err := broker.Connect(); err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer broker.Close()
    
    // Your application logic here...
}
```

## Usage Examples

### Configuration with Options

```go
broker := rabbitmq.NewBroker(
    "amqp://guest:guest@localhost:5672/",
    rabbitmq.WithReconnectDelay(10 * time.Second),
    rabbitmq.WithPrefetch(20),
    rabbitmq.WithQueues([]rabbitmq.QueueConfig{
        {
            Name:        "user.events",
            Exchange:    "events",
            RoutingKeys: []string{"user.created", "user.updated"},
        },
        {
            Name:        "order.queue",
            Exchange:    "orders",
            RoutingKeys: []string{"order.created", "order.completed"},
        },
    }),
)
```

### Publishing Messages

```go
ctx := context.Background()

// Publish a message
message := []byte(`{"id": 123, "name": "John Doe"}`)
err := broker.Publish(ctx, "events", "user.created", message)
if err != nil {
    log.Printf("Failed to publish: %v", err)
}
```

### Consuming Messages

#### Simple Consumer

```go
handler := func(ctx *rabbitmq.Context) error {
    var user struct {
        ID   int    `json:"id"`
        Name string `json:"name"`
    }
    
    // Bind JSON payload
    if err := ctx.BindJSON(&user); err != nil {
        return err
    }
    
    log.Printf("Received user: %+v", user)
    
    // Process the message...
    
    // Acknowledge the message
    return ctx.Ack()
}

// Consume with 5 workers
err := broker.Consume("user.events", 5, handler)
if err != nil {
    log.Fatal("Failed to consume:", err)
}
```

#### Error Handling

```go
handler := func(ctx *rabbitmq.Context) error {
    // Your processing logic
    if err := processMessage(ctx); err != nil {
        // Reject message (won't be requeued by default)
        ctx.Nack(false)
        return err
    }
    
    // Success - acknowledge
    return ctx.Ack()
}
```

### Using the Router

The router allows you to handle different event types from a single queue:

```go
router := rabbitmq.NewRouter()

// Register handlers for different event types
router.Handle("user.created", func(ctx *rabbitmq.Context) error {
    var user User
    if err := ctx.BindJSON(&user); err != nil {
        return err
    }
    
    log.Printf("User created: %+v", user)
    // Handle user creation...
    
    return ctx.Ack()
})

router.Handle("user.updated", func(ctx *rabbitmq.Context) error {
    var user User
    if err := ctx.BindJSON(&user); err != nil {
        return err
    }
    
    log.Printf("User updated: %+v", user)
    // Handle user update...
    
    return ctx.Ack()
})

// Consume messages and route them
handler := func(ctx *rabbitmq.Context) error {
    return router.Execute(ctx)
}

err := broker.Consume("user.events", 3, handler)
```

**Note**: For the router to work, your messages must have a `type` field in the JSON payload:

```json
{
  "type": "user.created",
  "id": 123,
  "name": "John Doe"
}
```

### Accessing Message Headers

```go
handler := func(ctx *rabbitmq.Context) error {
    retries := ctx.GetHeader("x-retries")
    if retries != nil {
        log.Printf("Message retry count: %v", retries)
    }
    
    // Process message...
    return ctx.Ack()
}
```

## Configuration Options

### `WithReconnectDelay(d time.Duration)`

Sets the delay between reconnection attempts. Default: `5 seconds`.

```go
broker := rabbitmq.NewBroker(
    "amqp://localhost:5672/",
    rabbitmq.WithReconnectDelay(10 * time.Second),
)
```

### `WithPrefetch(count int)`

Sets the number of unacknowledged messages that can be delivered to a consumer. Default: `10`.

```go
broker := rabbitmq.NewBroker(
    "amqp://localhost:5672/",
    rabbitmq.WithPrefetch(20),
)
```

### `WithQueues(queues []QueueConfig)`

Configures the topology (exchanges, queues, and bindings) to be automatically created on connection.

```go
broker := rabbitmq.NewBroker(
    "amqp://localhost:5672/",
    rabbitmq.WithQueues([]rabbitmq.QueueConfig{
        {
            Name:        "my.queue",
            Exchange:    "my.exchange",
            RoutingKeys: []string{"key1", "key2"},
        },
    }),
)
```

## Complete Example

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/edaniel30/rabbitmq-kit-go"
)

func main() {
    // Configure broker with topology
    broker := rabbitmq.NewBroker(
        "amqp://guest:guest@localhost:5672/",
        rabbitmq.WithReconnectDelay(5 * time.Second),
        rabbitmq.WithPrefetch(10),
        rabbitmq.WithQueues([]rabbitmq.QueueConfig{
            {
                Name:        "tasks",
                Exchange:    "task-exchange",
                RoutingKeys: []string{"task.high", "task.low"},
            },
        }),
    )
    
    // Connect
    if err := broker.Connect(); err != nil {
        log.Fatal("Failed to connect:", err)
    }
    defer broker.Close()
    
    // Setup router
    router := rabbitmq.NewRouter()
    
    router.Handle("task.high", func(ctx *rabbitmq.Context) error {
        var task Task
        ctx.BindJSON(&task)
        log.Printf("Processing high priority task: %+v", task)
        // Process task...
        return ctx.Ack()
    })
    
    router.Handle("task.low", func(ctx *rabbitmq.Context) error {
        var task Task
        ctx.BindJSON(&task)
        log.Printf("Processing low priority task: %+v", task)
        // Process task...
        return ctx.Ack()
    })
    
    // Start consuming
    err := broker.Consume("tasks", 5, router.Execute)
    if err != nil {
        log.Fatal("Failed to start consumer:", err)
    }
    
    // Publish a message
    ctx := context.Background()
    message := []byte(`{"type":"task.high","id":1,"data":"important task"}`)
    broker.Publish(ctx, "task-exchange", "task.high", message)
    
    // Keep application running
    select {}
}

type Task struct {
    ID   int    `json:"id"`
    Data string `json:"data"`
}
```

## Context Methods

### `BindJSON(v any) error`

Unmarshals the message body (JSON) into the provided struct.

### `Ack() error`

Acknowledges the message, removing it from the queue.

### `Nack(requeue bool) error`

Negatively acknowledges the message. If `requeue` is `true`, the message will be returned to the queue.

### `GetHeader(key string) any`

Retrieves a header value from the message.

## Requirements

- Go 1.25.5 or higher
- RabbitMQ server

## License

MIT
