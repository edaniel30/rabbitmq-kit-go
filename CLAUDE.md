# rabbitmq-kit-go - Developer Context Guide

## 1. Quick Project Overview

### Purpose
Production-ready RabbitMQ library for Go providing high-level abstractions for event-driven architectures following DDD patterns. Handles complexity of connection management, retries, circuit breaking, and dead letter queues.

### Tech Stack
- **Go 1.25+** with RabbitMQ 3.8+
- **Main Dependency:** `github.com/rabbitmq/amqp091-go` v1.10.0
- **Testing:** testify + testcontainers (Docker-based integration tests)

### Quick Start

```bash
# Setup
export RABBITMQ_URI="amqp://guest:guest@localhost:5672/"
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3.13-management-alpine

# Run examples
go run examples/basic/main.go

# Tests
make test-unit          # Fast, no Docker
make test               # All tests, requires Docker
make test-coverage      # With coverage report
```

---

## 2. Project Structure

```
rabbitmq-kit-go/
├── config/              # Configuration with functional options
├── errors/              # Custom error types
├── internal/            # Internal implementation (not exported)
│   ├── broker/         # Low-level AMQP operations
│   ├── circuitbreaker/ # Circuit breaker state machine
│   └── logger/         # Logging abstraction
├── router/              # Message routing by event type
├── testing/             # Test helpers (testcontainers)
├── examples/            # Working examples
├── docs/                # Detailed documentation
│   ├── CONFIGURATION.md
│   ├── CIRCUIT_BREAKER.md
│   ├── DLQ.md
│   └── BATCH_PUBLISHING.md
├── event.go            # Event interface & BaseEvent
├── eventbus.go         # EventBus - main public API
└── logger.go           # Logger interface
```

### Package Responsibilities

| Package | Responsibility | Visibility |
|---------|---------------|-----------|
| **Root** | EventBus, Event, BaseEvent | Public |
| `config` | Configuration types & options | Public |
| `errors` | Custom/sentinel errors | Public |
| `router` | Event routing & message context | Public |
| `internal/*` | AMQP operations, circuit breaker | Internal |
| `testing` | Test helpers | Public |

---

## 3. Architecture & Design

### Pattern: Clean Architecture + Facade

```
Public API (EventBus, Event, Config) ← Users interact here
    ↓
Router (Handler routing by event type)
    ↓
Internal/Broker (AMQP operations)      ← Hidden from users
    ↓
RabbitMQ (amqp091-go)
```

### Key Design Patterns

1. **Facade** (`eventbus.go`) - Hides internal complexity
2. **Functional Options** (`config/`) - Flexible, backward-compatible config
3. **Strategy** (`router/`) - User-defined handlers
4. **State Machine** (circuit breaker) - Closed/Open/HalfOpen states
5. **Worker Pool** (`internal/broker/consumer.go`) - Concurrent message processing
6. **Retry with Exponential Backoff** (`internal/broker/retry.go`)

### Core Principles

- **Simple Public API**: EventBus hides all complexity
- **Dependency Injection**: Via functional options
- **Layer Separation**: Public → Router → Internal/Broker
- **Type Safety**: Compile-time checking, no magic strings
- **Production Ready**: Retries, circuit breakers, DLQ, publisher confirms

---

## 4. Coding Conventions

### Naming

```go
// Exported: PascalCase
type EventBus struct {}
func NewEventBus() {}

// Unexported: camelCase
var maxRetries int
func (c *Client) connect() {}

// Acronyms keep casing
type DLQConfig struct {}  // NOT DlqConfig
func StartConsumeDLQ()    // NOT StartConsumeDlq

// Errors
var ErrClientClosed = errors.New("...")  // Sentinel errors
type PublishError struct {}              // Error types
```

### File Structure

1. Package declaration
2. Imports (stdlib → external → internal)
3. Constants
4. Types
5. Constructors (New*, Default*)
6. Methods (grouped by receiver)
7. Helpers (unexported)

### Comments

- **Godoc**: Full sentences for exported types/functions
- **Implementation**: Explain "why", not "what"
- **TODO**: Include issue number if available

---

## 5. Error Handling

### Error Types

```go
// Sentinel errors (check with errors.Is)
errors.ErrPublishNotConfirmed
errors.ErrMaxRetriesExceeded
errors.ErrClientClosed

// Typed errors (extract with errors.As)
*errors.PublishError
*errors.ConnectionError
*errors.ConfigError
```

### Pattern

```go
// Low level: Create typed errors
return errors.NewPublishError(exchange, routingKey, err)

// Mid level: Log and propagate
if err := eb.publisher.Publish(...); err != nil {
    eb.config.Logger.Error(ctx, "Failed to publish", map[string]any{
        "event_type": event.Type(),
        "error": err,
    })
    return err
}
```

### Logging

```go
// Structured logging with context
logger.Error(ctx, "Handler error", map[string]any{
    "worker_id": workerID,
    "event_type": eventType,
    "error": err,
})

// Levels: Debug (dev) → Info (normal) → Warn (degraded) → Error (failure)
```

---

## 6. Testing Strategy

### Test Types

```bash
# Unit tests (no Docker, <1s)
make test-unit
go test -short ./...

# Integration tests (Docker, ~30s)
make test

# Coverage (threshold: 80%)
make test-coverage
make test-coverage-html
```

### Patterns

```go
// Table-driven tests
tests := []struct {
    name        string
    input       Input
    expected    Expected
    expectError bool
}{
    {"success", Input{...}, Expected{...}, false},
    {"error", Input{...}, Expected{...}, true},
}

for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // Test logic
    })
}

// Use require for critical checks (stops test)
require.NoError(t, err)

// Use assert for soft checks (continues)
assert.Equal(t, expected, actual)
```

### Coverage Exclusions

- `/examples` - Example code
- `/testing` - Test helpers
- `/internal/mocks` - Mock implementations

Current: **81.0%** (threshold: 80%)

---

## 7. Common Operations

### Build & Run

```bash
# Run example
go run examples/basic/main.go

# Run specific test
go test -v -run TestEventBus_Integration ./...

# Race detector
make test-race
```

### Quality Checks

```bash
# All pre-commit checks
make pre-commit

# Linting
golangci-lint run
golangci-lint run --fix

# Format
gofmt -w .
go mod tidy
```

### Git Workflow

```bash
# Commit style: feat/fix/docs/test/refactor(scope): message
git commit -m "fix(ci): align coverage calculation"

# Pre-commit hooks
make setup  # Install once
```

---

## 8. Key Business Rules

### Event Routing
- Events MUST have `"type"` field in JSON for routing
- Event type = routing key
- Use `BaseEvent` for common fields (id, occurred_at, type)

### Retry Behavior
- Max retries default: 3
- Exponential backoff: 1s, 2s, 4s, 8s...
- After max retries: Send to DLX (if configured) or discard
- Retry count tracked in `x-retry-count` header

### Circuit Breaker
- **Applies to consumers only**, not publishers
- States: Closed (normal) → Open (blocking) → HalfOpen (testing)
- On open: Messages NACK'd → DLX
- See: `docs/CIRCUIT_BREAKER.md`

### Publisher Confirms
- If enabled: `Publish()` blocks until confirmed or timeout
- Default timeout: 5s
- NACK → `ErrPublishNotConfirmed`
- Timeout → `ErrPublishConfirmTimeout`

### DLQ Convention
- DLX exchange: `dlx.exchange` or custom
- DLQ queue: `dlq.{original_queue_name}`
- See: `docs/DLQ.md`

---

## 9. Performance Tuning

### Prefetch Count

```go
// Fast processing (<100ms): 50-100
config.WithPrefetchCount(100)

// Medium (100ms-1s): 10-20
config.WithPrefetchCount(10)

// Slow (>1s): 1-5
config.WithPrefetchCount(1)
```

### Worker Count

```go
// CPU-bound: num_cores
eventBus.StartConsume("queue", runtime.NumCPU())

// I/O-bound: 2-4x num_cores
eventBus.StartConsume("queue", runtime.NumCPU() * 3)
```

### Batch Publishing

```go
// Pipelined: 5-10x faster
eventBus.PublishBatch(ctx, events, config.WithPipelining(true))

// Async: Fastest for 1000+ events
eventBus.PublishBatchAsync(ctx, events, config.WithMaxConcurrency(50))
```

See: `docs/BATCH_PUBLISHING.md`

---

## 10. Anti-Patterns to Avoid

### ❌ Don't create EventBus per request

```go
// BAD: New connection per request
func HandleRequest(w http.ResponseWriter, r *http.Request) {
    eb, _ := rabbitmq.NewEventBus(...)  // ❌
    defer eb.Close()
}

// GOOD: Reuse EventBus
var globalEventBus *rabbitmq.EventBus
func init() {
    globalEventBus, _ = rabbitmq.NewEventBus(...)
}
```

### ❌ Don't block in handlers

```go
// BAD: Blocks worker
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    time.Sleep(10 * time.Second)  // ❌
    return nil
})

// GOOD: Quick processing or async
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    go processAsync(ctx)
    return nil  // ACK immediately
})
```

### ❌ Don't ignore context

```go
// BAD: No timeout
eventBus.Publish(context.Background(), event)  // ❌

// GOOD: Use timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
eventBus.Publish(ctx, event)  // ✓
```

### ❌ Don't panic in handlers

```go
// BAD: Crashes worker
panic("error")  // ❌

// GOOD: Return error
return fmt.Errorf("validation failed: %w", err)  // ✓
```

---

## 11. Security & Best Practices

### Connection Security

```go
// ❌ Don't hardcode credentials
config.WithURI("amqp://admin:password@prod:5672/")

// ✓ Use environment variables
config.WithURI(os.Getenv("RABBITMQ_URI"))

// ✓ Use TLS
config.WithURI("amqps://...")
```

### Message Security

- Library does NOT encrypt message bodies
- Encrypt before `Publish()` if needed
- Don't log sensitive data in errors

---

## 12. Adding New Features

### New Configuration Option

```go
// 1. Add field to Config struct (config/config.go)
type Config struct {
    NewFeature bool
}

// 2. Update DefaultConfig()
func DefaultConfig() Config {
    return Config{NewFeature: false}
}

// 3. Add functional option
func WithNewFeature(enabled bool) Option {
    return func(c *Config) { c.NewFeature = enabled }
}

// 4. Add validation if needed
// 5. Add tests
```

### New Event Type

```go
type OrderCompletedEvent struct {
    *rabbitmq.BaseEvent
    OrderID string
}

func (e *OrderCompletedEvent) ToMap() map[string]any {
    m := e.BaseEvent.ToMap()
    m["order_id"] = e.OrderID
    return m
}
```

### Checklist for New Public Methods

- [ ] Godoc comment with example
- [ ] Context parameter if I/O operation
- [ ] Return typed error
- [ ] Add logging for errors
- [ ] Implement in `internal/broker` first
- [ ] Unit + integration tests
- [ ] Update README.md if user-facing
- [ ] Add example if complex

---

## 13. Documentation References

- **README.md** - Quick start, basic examples
- **docs/CONFIGURATION.md** - All config options detailed
- **docs/CIRCUIT_BREAKER.md** - Circuit breaker setup & monitoring
- **docs/DLQ.md** - Dead letter queue configuration
- **docs/BATCH_PUBLISHING.md** - Batch publishing strategies
- **examples/** - Complete working examples for all features

---

## Quick Reference

### Most Used Commands

```bash
make test-unit          # Fast tests
make test-coverage      # Coverage report
make pre-commit         # Linting + format
go run examples/*/main.go  # Run examples
```

### Code Review Checklist

1. ✅ Tests pass (`make test`)
2. ✅ Coverage ≥ 80%
3. ✅ Linter passes (`make pre-commit`)
4. ✅ No breaking API changes
5. ✅ Godoc for public types/methods
6. ✅ Integration test for RabbitMQ interactions
7. ✅ Context support for I/O operations

### When in Doubt

- Check `examples/` for usage patterns
- Read godoc comments for behavior details
- Look at existing tests for patterns
- Use functional options for config
- Return typed errors from `errors/` package
