# rabbitmq-kit-go Examples

This directory contains practical, runnable examples demonstrating all major features of the rabbitmq-kit-go library.

## Prerequisites

Before running any example, ensure you have:

1. **RabbitMQ running** on `localhost:5672`
   ```bash
   # Using Docker
   docker run -d --name rabbitmq \
     -p 5672:5672 \
     -p 15672:15672 \
     rabbitmq:3-management
   ```

2. **Go installed** (1.21 or later)
   ```bash
   go version
   ```

3. **Dependencies installed**
   ```bash
   go mod download
   ```

## Environment Variables

All examples support the following environment variable:

- `RABBITMQ_URI` - Connection string (default: `amqp://guest:guest@localhost:5672/`)

Example:
```bash
export RABBITMQ_URI="amqp://user:password@localhost:5672/"
```

## Examples Overview

### 1. Basic Publisher/Consumer 📚
**Path:** `examples/basic/`

The essential starting point for using rabbitmq-kit-go.

**Features Demonstrated:**
- Creating an EventBus with configuration
- Defining custom events (implementing `Event` interface)
- Publishing events to exchanges
- Registering event handlers
- Consuming events with multiple workers
- Graceful shutdown

**Run:**
```bash
cd examples/basic
go run main.go
```

**What to expect:**
- 5 order events published
- Consumer processes each event
- Clean shutdown on Ctrl+C

---

### 2. Batch Publishing 🚀
**Path:** `examples/batch_publishing/`

High-performance batch operations for publishing multiple events efficiently.

**Features Demonstrated:**
- Pipelined batch publishing (5-10x faster)
- Sequential batch mode (for comparison)
- Async batch with worker pool control
- Handling partial failures in batches
- Performance metrics and throughput measurement

**Run:**
```bash
cd examples/batch_publishing
go run main.go
```

**What to expect:**
- 100 events published in <1 second
- Performance comparison between modes
- Throughput statistics displayed

**Key Learnings:**
- Pipelining: Send all, then wait for all confirms (fastest)
- Async: Good for when order doesn't matter
- Partial failures: Individual error tracking per message

---

### 3. Dead Letter Queue (DLQ) 💀
**Path:** `examples/dlq/`

Handling failed messages with automatic DLQ setup.

**Features Demonstrated:**
- Automatic DLQ setup with `WithDLQ(true)`
- DLQ naming convention (`dlq.` prefix)
- Consuming from DLQ for analysis
- Extracting failure metadata (retry count, death reason, original queue)
- Re-enqueuing failed messages
- Bulk re-enqueue with `RequeueAllFromDLQ()`

**Run:**
```bash
cd examples/dlq
go run main.go
```

**What to expect:**
- Some payment events fail intentionally
- Failed messages route to `dlq.payments.queue`
- DLQ handler analyzes failures
- Messages re-enqueued after analysis

**Key Learnings:**
- DLQ receives messages after max retries exhausted
- DLQMessage provides rich failure metadata
- Can selectively retry or permanently discard

---

### 4. Circuit Breaker ⚡
**Path:** `examples/circuit_breaker/`

Protecting your consumers from cascading failures.

**Features Demonstrated:**
- Enabling circuit breaker with configuration
- Circuit breaker states: Closed → Open → Half-Open → Closed
- Monitoring circuit breaker metrics in real-time
- Manual circuit breaker reset
- Automatic recovery after timeout

**Run:**
```bash
cd examples/circuit_breaker
go run main.go
```

**What to expect:**
- Phase 1: Circuit opens after 5 failures
- Phase 2: Circuit attempts half-open after 10s
- Phase 3: Successful messages close circuit
- Real-time metrics displayed every 2s

**Key Learnings:**
- **Closed**: Normal operation
- **Open**: All messages rejected (protects downstream)
- **Half-Open**: Testing recovery with limited requests

---

### 5. Retry Mechanism 🔄
**Path:** `examples/retry/`

Automatic retry with exponential backoff for transient failures.

**Features Demonstrated:**
- Configuring `MaxRetries` (attempts before DLQ)
- Tracking retry attempts via `x-retry-count` header
- Accessing retry count in handlers
- Integration with DLQ after max retries
- Simulating transient vs permanent failures

**Run:**
```bash
cd examples/retry
go run main.go
```

**What to expect:**
- Emails fail with ~70% initial success rate
- Automatic retries up to 3 times
- Success rate improves with retries
- Permanently failed emails sent to DLQ

**Key Learnings:**
- Retries are automatic, no code needed
- Retry count accessible via `ctx.GetRetryCount()`
- After max retries → DLQ
- Great for network errors, rate limits, temp failures

---

### 6. Publisher Confirms 🔒
**Path:** `examples/publisher_confirms/`

Guaranteed message delivery with RabbitMQ publisher confirms.

**Features Demonstrated:**
- Enabling publisher confirms
- Synchronous confirmation waiting
- High-performance asynchronous confirms
- Handling ACK/NACK from broker
- Batch publishing with confirms (pipelining)
- Throughput comparison with/without confirms

**Run:**
```bash
cd examples/publisher_confirms
go run main.go
```

**What to expect:**
- Example 1: Fast publish without confirms (no guarantee)
- Example 2: Confirmed publish (slower but guaranteed)
- Example 3: Batch with confirms (high throughput + guarantee)

**Key Learnings:**
- Publisher confirms = guaranteed delivery
- Asynchronous system achieves high throughput
- Trade-off: ~10-20% slower but 100% reliable
- Essential for financial transactions, orders, etc.

---

## Running All Examples

To run all examples sequentially:

```bash
# From examples directory
for dir in basic batch_publishing dlq circuit_breaker retry publisher_confirms; do
  echo "Running $dir..."
  (cd $dir && go run main.go) &
  sleep 15
  pkill -f "go run main.go"
done
```

## Common Patterns

### Event Implementation

All examples follow this pattern for custom events:

```go
type MyEvent struct {
    Field1 string `json:"field1"`
    Field2 int    `json:"field2"`
}

func (e MyEvent) Type() string      { return "my.event.type" }
func (e MyEvent) Exchange() string  { return "my.exchange" }
func (e MyEvent) ToMap() map[string]any {
    return map[string]any{
        "type":   e.Type(),
        "field1": e.Field1,
        "field2": e.Field2,
    }
}
```

### Handler Implementation

```go
type MyHandler struct{}

func (h *MyHandler) Execute(ctx *router.MessageContext) error {
    var data map[string]any
    if err := ctx.BindJSON(&data); err != nil {
        return err
    }

    // Process message
    log.Printf("Processing: %v", data)

    return nil // Or return error to trigger retry
}
```

### Configuration Template

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://localhost:5672/"),
    config.WithMaxRetries(3),
    config.WithPublisherConfirms(true),
    config.WithDLQ(true),
    config.WithCircuitBreaker(true),
    config.WithExchanges([]config.ExchangeConfig{
        {Name: "my.exchange", Type: "direct", Durable: true},
    }),
    config.WithQueues([]config.QueueConfig{
        {
            Name:        "my.queue",
            Exchange:    "my.exchange",
            RoutingKeys: []string{"my.routing.key"},
            Durable:     true,
        },
    }),
)
```

## Troubleshooting

### Connection Refused
```
Failed to create EventBus: dial tcp: connect: connection refused
```
**Solution:** Ensure RabbitMQ is running on port 5672.

### Channel/Connection Closed
**Solution:** Check RabbitMQ logs for errors. May need to increase memory/resources.

### Messages Not Consuming
**Solution:**
- Verify queue exists in RabbitMQ management UI (http://localhost:15672)
- Check handler is registered before `StartConsume()`
- Ensure routing keys match

### Slow Performance
**Solution:**
- Enable publisher confirms only when needed
- Use pipelined batch publishing for bulk operations
- Increase worker count in `StartConsume()`

## Next Steps

After running these examples:

1. **Read the main README** for full API documentation
2. **Review the source code** to understand implementation details
3. **Build your own** event-driven application
4. **Explore advanced features** like custom loggers, topology management

## Support

- **Issues:** https://github.com/edaniel30/rabbitmq-kit-go/issues
- **Documentation:** See main README.md
- **RabbitMQ Docs:** https://www.rabbitmq.com/documentation.html

## License

MIT License - See LICENSE file for details
