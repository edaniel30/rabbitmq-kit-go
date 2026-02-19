# Circuit Breaker

Protect your services from cascading failures with automatic circuit breaking.

## Overview

The circuit breaker pattern prevents a consumer from repeatedly processing messages when the downstream service is failing. Instead of continuing to fail, the circuit "opens" and rejects messages temporarily, allowing the downstream service to recover.

## States

```
CLOSED (normal) ──> OPEN (failing) ──> HALF-OPEN (testing) ──> CLOSED
     │                   │                    │
     │                   │                    └─> OPEN (still failing)
     │                   └─> Wait resetTimeout
     └─> After maxFailures
```

### CLOSED (Normal Operation)

- All messages are processed
- Failures are counted
- When failures >= `maxFailures` → transition to OPEN

### OPEN (Circuit Broken)

- All messages are **rejected immediately** (NACK without requeue)
- No handler execution
- After `resetTimeout` → transition to HALF-OPEN

### HALF-OPEN (Testing Recovery)

- Allows `halfOpenRequests` test messages
- If all succeed → transition to CLOSED
- If any fails → transition back to OPEN

## Configuration

```go
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithCircuitBreaker(
        true,              // enabled
        5,                 // maxFailures (before opening)
        10*time.Second,    // resetTimeout (before testing)
        3,                 // halfOpenRequests (test requests)
    ),
)
```

**Parameters**:
- `enabled`: Enable circuit breaker
- `maxFailures`: Number of consecutive failures before opening circuit
- `resetTimeout`: Wait time before attempting recovery (HALF-OPEN)
- `halfOpenRequests`: Number of test requests allowed in HALF-OPEN state

## Usage Example

```go
package main

import (
    "log"
    "time"

    "github.com/edaniel30/rabbitmq-kit-go"
    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/router"
)

func main() {
    eventBus, _ := rabbitmq.NewEventBus(
        config.DefaultConfig(),
        config.WithURI("amqp://localhost:5672/"),
        config.WithCircuitBreaker(true, 5, 10*time.Second, 3),
    )
    defer eventBus.Close()

    // Handler that may fail
    eventBus.RegisterHandler("api.call", func(ctx *router.MessageContext) error {
        // Simulate calling external API
        if err := callExternalAPI(); err != nil {
            return err // Will trigger circuit breaker
        }
        return nil
    })

    eventBus.StartConsume("api.queue", 5)

    // Monitor circuit breaker metrics
    go monitorCircuitBreaker(eventBus)

    select {}
}

func monitorCircuitBreaker(eb *rabbitmq.EventBus) {
    ticker := time.NewTicker(2 * time.Second)
    for range ticker.C {
        metrics := eb.GetCircuitBreakerMetrics()
        if metrics != nil {
            log.Printf("Circuit Breaker: %s (failures=%d, successes=%d)",
                metrics.State, metrics.Failures, metrics.Successes)
        }
    }
}
```

## Monitoring

### Get Metrics

```go
metrics := eventBus.GetCircuitBreakerMetrics()
if metrics != nil {
    log.Printf("State: %s", metrics.State)           // closed, open, half-open
    log.Printf("Failures: %d", metrics.Failures)     // Consecutive failures
    log.Printf("Successes: %d", metrics.Successes)   // Recent successes
    log.Printf("Last Failure: %v", metrics.LastFailureTime)
}
```

### Manual Reset

Force circuit breaker to close (use with caution):

```go
if eventBus.ResetCircuitBreaker() {
    log.Println("Circuit breaker manually reset to CLOSED")
}
```

## Behavior Examples

### Scenario 1: Downstream Service Fails

```
1. Handler fails (error returned)          → Failures: 1
2. Handler fails                           → Failures: 2
3. Handler fails                           → Failures: 3
4. Handler fails                           → Failures: 4
5. Handler fails                           → Failures: 5
   → Circuit opens (state: OPEN)
6. Messages rejected immediately           → No handler execution
7. Wait 10s (resetTimeout)
   → Circuit transitions to HALF-OPEN
8. Allow 3 test messages (halfOpenRequests)
9. Test message succeeds                   → Successes: 1
10. Test message succeeds                  → Successes: 2
11. Test message succeeds                  → Successes: 3
    → Circuit closes (state: CLOSED)
```

### Scenario 2: Recovery Fails

```
1-5. Failures → Circuit OPEN
6. Wait 10s → Circuit HALF-OPEN
7. Test message succeeds                   → Successes: 1
8. Test message fails                      → Circuit OPEN again
9. Wait another 10s...
```

## Integration with Retry Mechanism

Circuit breaker works alongside the retry mechanism:

```go
config.WithMaxRetries(3)
config.WithCircuitBreaker(true, 5, 10*time.Second, 3)
```

**Flow**:
1. Message fails → Retry (up to 3 times)
2. Each retry failure → Circuit breaker counts failure
3. After 5 failures → Circuit opens
4. New messages rejected → No retry attempts

## Best Practices

1. **Set appropriate maxFailures**:
   - Too low: Circuit opens too easily (false positives)
   - Too high: More damage before opening
   - Recommendation: 5-10 for production

2. **Choose resetTimeout wisely**:
   - Too short: Circuit tests recovery too early
   - Too long: Slow recovery
   - Recommendation: 10-30s for most services

3. **Configure halfOpenRequests**:
   - Enough to confirm recovery: 3-5
   - Too many: Risk of overloading recovering service

4. **Monitor circuit breaker state**:
   ```go
   if metrics.State == "open" {
       alert.Send("Circuit breaker open for queue: " + queueName)
   }
   ```

5. **Combine with DLQ**:
   - Messages rejected by circuit breaker go to DLQ
   - Allows manual inspection and requeue after fix

6. **Use with health checks**:
   ```go
   eventBus.RegisterHandler("health.check", func(ctx *router.MessageContext) error {
       return checkDownstreamService() // Affects circuit breaker
   })
   ```

## Common Configurations

### Conservative (Slow to Open)

```go
config.WithCircuitBreaker(true, 10, 30*time.Second, 5)
```

Use when: Transient failures are common.

### Aggressive (Fast to Open)

```go
config.WithCircuitBreaker(true, 3, 10*time.Second, 2)
```

Use when: Failures indicate serious problems.

### Balanced (Production Default)

```go
config.WithCircuitBreaker(true, 5, 10*time.Second, 3)
```

Use when: General production workloads.

## Limitations

- Circuit breaker is **per-consumer** (not global across instances)
- Requires consistent failure patterns to be effective
- Works best with **consistent failure modes** (e.g., database down, API unreachable)
- May not help with **intermittent failures** or **poison messages**