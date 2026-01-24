# Dead Letter Queue (DLQ)

Handle failed messages, analyze poison messages, and implement retry strategies.

## Overview

A Dead Letter Queue (DLQ) receives messages that couldn't be processed after all retry attempts. This allows:
- Manual inspection of failed messages
- Analysis of failure patterns
- Selective requeuing after fixes
- Permanent storage of poison messages

## How It Works

```
1. Message fails → Retry (up to MaxRetries)
2. Max retries exceeded → NACK without requeue
3. Message sent to DLX (Dead Letter Exchange)
4. DLX routes to DLQ
5. DLQ consumer processes/analyzes/requeues
```

## Quick Setup

### Auto-setup DLQ

Easiest way - automatically creates DLQ infrastructure:

```go
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://localhost:5672/"),
    config.WithDLQEnabled(true, "dlq."),
)
```

**Creates**:
- DLX exchange: `dlq.exchange`
- DLQ for each queue: `dlq.{queue_name}`
- Automatic bindings

### Manual Setup

For custom DLQ configuration:

```go
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithExchanges([]config.ExchangeConfig{
        {Name: "orders.exchange", Type: "topic", Durable: true},
        {Name: "orders.dlx", Type: "topic", Durable: true},  // DLX
    }),
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
            Name:        "orders.dlq",           // Dead Letter Queue
            Exchange:    "orders.dlx",
            RoutingKeys: []string{"failed.orders"},
            Durable:     true,
        },
    }),
)
```

## Consuming DLQ Messages

### Register DLQ Handler

```go
eventBus.RegisterDLQHandler("order.created", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    log.Printf("Failed message: %s", dlqMsg.GetDeathInfo())
    log.Printf("Retry count: %d", dlqMsg.RetryCount)
    log.Printf("Original queue: %s", dlqMsg.OriginalQueue)
    log.Printf("Failure reason: %s", dlqMsg.Reason)
    log.Printf("Failed at: %v", dlqMsg.FirstDeathTimestamp)

    // Analyze and decide what to do
    if dlqMsg.ShouldRetry(10) {
        // Requeue with reset retry count
        return eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
    }

    // Or log and discard
    log.Printf("Permanently failed: %s", string(dlqMsg.Body()))
    return nil
})

// Start consuming DLQ
eventBus.StartConsumeDLQ("dlq.orders.queue", 1)
```

### DLQMessage Fields

```go
type DLQMessage struct {
    Delivery              amqp.Delivery
    OriginalExchange      string    // Where message was originally published
    OriginalRoutingKey    string    // Original routing key
    OriginalQueue         string    // Queue where it failed
    RetryCount            int       // Number of retry attempts
    Reason                string    // Failure reason (rejected, expired, etc.)
    DeathCount            int64     // Times sent to DLX
    FirstDeathTimestamp   time.Time // First time it failed
}
```

## Requeuing Messages

### Requeue Single Message

```go
eventBus.RegisterDLQHandler("user.created", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    // Check if we should retry
    if dlqMsg.ShouldRetry(maxRetries) {
        // Requeue to original queue
        err := eventBus.RequeueFromDLQ(
            context.Background(),
            dlqMsg,
            true, // reset retry count
        )
        if err != nil {
            return err
        }

        // ACK the DLQ message after successful requeue
        return ctx.Ack()
    }

    return nil // Discard
})
```

### Bulk Requeue

Requeue all messages from DLQ (useful after fixing a bug):

```go
count, err := eventBus.RequeueAllFromDLQ(
    context.Background(),
    "dlq.orders.queue",  // DLQ name
    true,                 // reset retry count
    100,                  // max messages (0 = unlimited)
)
if err != nil {
    log.Printf("Requeue error: %v", err)
}
log.Printf("Requeued %d messages", count)
```

## Analysis and Monitoring

### Get Death Information

```go
dlqMsg := router.NewDLQMessage(ctx)

info := dlqMsg.GetDeathInfo()
// Returns formatted string:
// "queue=orders.queue reason=rejected count=1 time=2026-01-24T10:00:00Z"
```

### Check if Should Retry

```go
if dlqMsg.ShouldRetry(10) {
    // Less than 10 total attempts (including original tries)
    log.Println("Safe to retry")
} else {
    // Too many attempts - probably poison message
    log.Println("Do not retry - analyze or discard")
}
```

### Access Message Body

```go
body := dlqMsg.Body()
log.Printf("Message body: %s", string(body))

// Parse JSON
var data map[string]any
json.Unmarshal(body, &data)
```

## Common Patterns

### Pattern 1: Automatic Requeue with Limit

```go
eventBus.RegisterDLQHandler("order.created", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    // Requeue if less than 10 total attempts
    if dlqMsg.ShouldRetry(10) {
        log.Printf("Requeuing message (attempt %d)", dlqMsg.RetryCount)
        eventBus.RequeueFromDLQ(context.Background(), dlqMsg, false)
        return ctx.Ack()
    }

    // Permanently failed - store for analysis
    storeFailedMessage(dlqMsg)
    return ctx.Ack()
})
```

### Pattern 2: Conditional Requeue

```go
eventBus.RegisterDLQHandler("payment.process", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    // Check failure reason
    if dlqMsg.Reason == "rejected" {
        // Bug fixed - safe to requeue
        if isServiceHealthy() {
            eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
            return ctx.Ack()
        }
    }

    // Keep in DLQ for now
    return ctx.Nack(true) // Requeue in DLQ
})
```

### Pattern 3: Alert on Poison Messages

```go
eventBus.RegisterDLQHandler("critical.event", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    if !dlqMsg.ShouldRetry(5) {
        // Poison message detected
        alert.Send("Poison message in DLQ", map[string]any{
            "queue":       dlqMsg.OriginalQueue,
            "retry_count": dlqMsg.RetryCount,
            "body":        string(dlqMsg.Body()),
        })
    }

    return nil // Keep in DLQ for manual inspection
})
```

### Pattern 4: Time-based Retry

```go
eventBus.RegisterDLQHandler("scheduled.task", func(ctx *router.MessageContext) error {
    dlqMsg := router.NewDLQMessage(ctx)

    // Wait at least 1 hour before retrying
    timeSinceFailure := time.Since(dlqMsg.FirstDeathTimestamp)
    if timeSinceFailure < time.Hour {
        log.Printf("Too soon to retry, waiting...")
        return ctx.Nack(true) // Keep in DLQ
    }

    // Requeue after cooldown
    eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
    return ctx.Ack()
})
```

## Best Practices

1. **Always consume DLQ**:
   ```go
   go eventBus.StartConsumeDLQ("dlq.orders.queue", 1)
   ```

2. **Set retry limits**:
   ```go
   if !dlqMsg.ShouldRetry(10) {
       // Store permanently or alert
   }
   ```

3. **Monitor DLQ depth**:
   - Use RabbitMQ Management API
   - Alert when DLQ grows beyond threshold
   - Indicates systemic problems

4. **Log DLQ messages**:
   ```go
   log.Printf("DLQ: queue=%s, reason=%s, body=%s",
       dlqMsg.OriginalQueue,
       dlqMsg.Reason,
       string(dlqMsg.Body()),
   )
   ```

5. **Reset retry count carefully**:
   - `resetRetryCount=true`: Fresh start (use after bug fix)
   - `resetRetryCount=false`: Keep count (use for transient issues)

6. **Use separate DLQ consumers**:
   - Run DLQ consumers in separate process
   - Lower concurrency (usually 1 worker)
   - Allows main consumers to focus on new messages

## Troubleshooting

### Messages Not Going to DLQ

Check:
1. Queue has `x-dead-letter-exchange` configured
2. DLX exchange exists
3. DLQ is bound to DLX
4. Messages are being NACK'd without requeue

### Messages Stuck in DLQ

Check:
1. DLQ consumer is running
2. DLQ handler is registered for event type
3. Handler is returning without error

### Infinite Requeue Loop

Fix:
```go
// Always check retry count
if !dlqMsg.ShouldRetry(maxRetries) {
    return nil // Stop requeuing
}
```

## Complete Example

See [examples/dlq/](../examples/dlq/) for a working example with monitoring and requeuing.
