# Batch Publishing

High-performance batch publishing with pipelining.

## Overview

Publishing messages in batches provides significant performance improvements over sequential publishing. This library offers two batch publishing modes:

1. **Sequential** - Publish one by one (legacy, slowest)
2. **Pipelined** - Send all, then wait for all confirms (5-10x faster)

## Publishing Modes

### Sequential (Legacy)

Default mode - publishes messages one by one:

```go
events := []Event{event1, event2, event3}

result, err := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(false),
)
```

**When to use**: When strict message ordering is required.

**Performance**: ~100-200 msg/sec

### Pipelined Batch

Send all messages first, then collect all confirmations:

```go
events := []Event{event1, event2, event3}

result, err := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(true), // Default
)
```

**How it works**:
1. Publish ALL messages without waiting
2. Collect delivery tags
3. Wait for ALL confirmations in batch

**When to use**:
- Large batches (>10 messages)
- Publisher confirms enabled
- Order matters

**Performance**: ~1000-2000 msg/sec (5-10x faster)

## Batch Result

All batch methods return `BatchResult`:

```go
type BatchResult struct {
    Total   int          // Total messages in batch
    Success int          // Successfully published
    Failed  int          // Failed to publish
    Errors  []BatchError // Details of failures
}

type BatchError struct {
    Index int   // Position in original batch
    Event Event // The failed event
    Error error // What went wrong
}
```

### Handle Results

```go
result, err := eventBus.PublishBatch(ctx, events)
if err != nil {
    log.Fatal(err) // Fatal error (connection lost, etc.)
}

if result.Failed > 0 {
    log.Printf("Published %d/%d messages", result.Success, result.Total)

    for _, batchErr := range result.Errors {
        log.Printf("Message %d failed: %v", batchErr.Index, batchErr.Error)

        // Retry individual message
        eventBus.Publish(ctx, batchErr.Event)
    }
}
```

## Options

### WithPipelining

Enable/disable pipelined mode:

```go
// Pipelined (fast)
config.WithPipelining(true)

// Sequential (slow)
config.WithPipelining(false)
```

**Default**: `true`

### WithFailFast

Stop at first error:

```go
result, err := eventBus.PublishBatch(ctx, events,
    config.WithFailFast(true),
)
// Returns immediately on first failure
```

**Default**: `false` (continues on errors)

## Performance Comparison

Benchmark with 1000 messages, publisher confirms enabled:

| Method | Messages/sec | Time (1000 msgs) | Notes |
|--------|--------------|------------------|-------|
| Sequential | 150 | ~6.6s | One by one |
| Pipelined | 1500 | ~0.66s | **5-10x faster** |

## Use Cases

### Use Case 1: Domain Events from Aggregate

Publish accumulated domain events after database commit:

```go
// In aggregate
order.AddProduct(productID, qty)
order.RemoveProduct(otherID)
order.Complete()

// In repository (after DB commit)
events := order.PullEvents() // []Event

// Use pipelined batch (maintains order, fast)
result, err := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(true),
)
```

### Use Case 2: Bulk Data Import

Publish thousands of events from data import:

```go
events := make([]Event, 0, 10000)
for _, record := range records {
    events = append(events, NewRecordImportedEvent(record))
}

// Use pipelined batch in chunks for large volumes
const chunkSize = 1000
for i := 0; i < len(events); i += chunkSize {
    end := i + chunkSize
    if end > len(events) {
        end = len(events)
    }
    result, err := eventBus.PublishBatch(ctx, events[i:end],
        config.WithPipelining(true),
    )
    if err != nil || result.Failed > 0 {
        log.Printf("Chunk %d had failures", i/chunkSize)
    }
}
```

### Use Case 3: Event Replay

Replay events from event store:

```go
events := eventStore.LoadEvents(startPosition, endPosition)

// Use pipelined batch (maintain order, fast)
result, err := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(true),
    config.WithFailFast(true), // Stop on first error
)
```

## Error Handling

### Partial Failures

```go
result, err := eventBus.PublishBatch(ctx, events)
if err != nil {
    // Fatal error - connection lost, etc.
    log.Fatal(err)
}

if result.Failed > 0 {
    // Some messages failed
    log.Printf("Failed: %d/%d messages", result.Failed, result.Total)

    // Retry failed messages
    failedEvents := make([]Event, 0, result.Failed)
    for _, batchErr := range result.Errors {
        failedEvents = append(failedEvents, batchErr.Event)
    }

    // Retry with exponential backoff
    time.Sleep(time.Second)
    eventBus.PublishBatch(ctx, failedEvents)
}
```

### Fatal Errors

```go
result, err := eventBus.PublishBatch(ctx, events)
if err != nil {
    // Connection lost, client closed, etc.
    log.Printf("Fatal error: %v", err)
    // Retry entire batch
    return err
}
```

## Common Patterns

### Pattern 1: Chunked Publishing

For very large batches, publish in chunks:

```go
const chunkSize = 1000

for i := 0; i < len(events); i += chunkSize {
    end := i + chunkSize
    if end > len(events) {
        end = len(events)
    }

    chunk := events[i:end]
    result, err := eventBus.PublishBatch(ctx, chunk,
        config.WithPipelining(true),
    )

    if err != nil || result.Failed > 0 {
        log.Printf("Chunk %d failed", i/chunkSize)
    }
}
```

### Pattern 2: Rate-Limited Publishing

Control publishing rate:

```go
limiter := time.NewTicker(100 * time.Millisecond)
defer limiter.Stop()

for i := 0; i < len(events); i += 10 {
    <-limiter.C // Rate limit

    chunk := events[i:min(i+10, len(events))]
    eventBus.PublishBatch(ctx, chunk)
}
```

### Pattern 3: Parallel Batch Publishing

Publish multiple batches in parallel:

```go
var wg sync.WaitGroup
batches := splitIntoBatches(events, 1000)

for _, batch := range batches {
    wg.Add(1)
    go func(b []Event) {
        defer wg.Done()
        eventBus.PublishBatch(ctx, b)
    }(batch)
}

wg.Wait()
```

## Best Practices

1. **Enable Publisher Confirms**:
   ```go
   config.WithPublisherConfirms(true)
   ```
   Pipelining requires confirms for reliability.

2. **Choose appropriate batch size**:
   - Small batches (<10): Use single `Publish()`
   - Medium/large batches (≥10): Use `PublishBatch` with pipelining, chunk into 1000s for very large volumes

3. **Handle partial failures**:
   ```go
   if result.Failed > 0 {
       // Retry or log failed events
   }
   ```

4. **Use context timeouts**:
   ```go
   ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
   defer cancel()
   ```

5. **Monitor batch results**:
   ```go
   metrics.RecordBatchPublish(result.Total, result.Success, result.Failed)
   ```

6. **Avoid very large batches**:
   - Split into chunks of 1000-5000 messages
   - Prevents memory issues
   - Better error recovery

## Limitations

### Pipelined Batch

- Requires **publisher confirms** enabled
- All messages share same RabbitMQ channel (bottleneck)
- Memory usage: O(n) for delivery tags

### Async Worker Pool

- **Messages may be out of order**
- Limited by single RabbitMQ channel
- No ordering guarantees between workers

### Both

- Bottleneck: Single RabbitMQ channel
  - Solution: Use multiple EventBus instances
- Memory usage for large batches
  - Solution: Chunk into smaller batches

## When to Use What

```
Message count < 10
  └─> Use single Publish()

10 ≤ Messages < 100, order matters
  └─> Use PublishBatch with pipelining

Messages ≥ 100, order doesn't matter
  └─> Use PublishBatchAsync with worker pool

Messages ≥ 100, order matters
  └─> Use PublishBatch with pipelining, chunk into batches of 1000
```