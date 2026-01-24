# rabbitmq-kit-go - Developer Context Guide

## 1. Project Overview

### Purpose
`rabbitmq-kit-go` is a production-ready RabbitMQ library for Go that provides high-level abstractions for event-driven architectures following Domain-Driven Design (DDD) patterns. It simplifies RabbitMQ operations by providing a clean API for publishing and consuming domain events while handling the complexity of connection management, retries, circuit breaking, and dead letter queues.

### Tech Stack
- **Language**: Go 1.25+
- **Message Broker**: RabbitMQ 3.8+
- **Key Dependencies**:
  - `github.com/rabbitmq/amqp091-go` v1.10.0 - Official RabbitMQ Go client
  - `github.com/google/uuid` v1.6.0 - UUID generation for event IDs
  - `github.com/stretchr/testify` v1.11.1 - Testing assertions
  - `github.com/testcontainers/testcontainers-go` v0.40.0 - Integration testing with Docker

### Entry Points

**Public API Entry Point**:
```go
// Create EventBus (main entry point for users)
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://guest:guest@localhost:5672/"),
)
defer eventBus.Close()
```

**Running Examples**:
```bash
# Set RabbitMQ URI
export RABBITMQ_URI="amqp://guest:guest@localhost:5672/"

# Run specific example
go run examples/basic/main.go
go run examples/batch_publishing/main.go
go run examples/circuit_breaker/main.go
go run examples/dlq/main.go
```

**Running Tests**:
```bash
# All tests (requires Docker for integration tests)
make test

# Unit tests only (fast, no Docker)
make test-unit

# With coverage report
make test-coverage

# View HTML coverage report
make test-coverage-html
```

### Environment Setup
1. **Go 1.25+** installed
2. **RabbitMQ** running locally or accessible via URI:
   ```bash
   # Using Docker
   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management-alpine
   ```
3. **Pre-commit hooks** (optional but recommended):
   ```bash
   pip install pre-commit
   make setup  # Installs hooks
   ```

---

## 2. Project Structure

```
rabbitmq-kit-go/
├── config/                    # Configuration package
│   ├── config.go             # Core config types and functional options
│   ├── exchange.go           # Exchange configuration
│   ├── queue.go              # Queue configuration
│   ├── batch.go              # Batch publishing options
│   └── dlq.go                # Dead Letter Queue configuration
│
├── errors/                    # Custom error types
│   └── errors.go             # Sentinel errors and typed errors
│
├── internal/                  # Internal implementation (not exported)
│   ├── broker/               # Low-level RabbitMQ operations
│   │   ├── client.go         # Connection and channel management
│   │   ├── publisher.go      # Message publishing with confirms
│   │   ├── consumer.go       # Message consumption with workers
│   │   └── retry.go          # Retry logic with exponential backoff
│   │
│   ├── circuitbreaker/       # Circuit breaker pattern implementation
│   │   ├── circuitbreaker.go # State machine (Closed/Open/HalfOpen)
│   │   └── config.go         # Circuit breaker configuration
│   │
│   └── logger/               # Logging abstraction
│       └── default_logger.go # Default stdout/stderr logger
│
├── router/                    # Message routing by event type
│   ├── router.go             # Handler registration and lookup
│   ├── message.go            # MessageContext wrapper for AMQP delivery
│   └── dlqMessage.go         # DLQ-specific message wrapper
│
├── testing/                   # Test utilities
│   └── testhelpers.go        # RabbitMQ testcontainer setup
│
├── examples/                  # Complete working examples
│   ├── basic/                # Simple publish/consume
│   ├── batch_publishing/     # Pipelined and async batching
│   ├── circuit_breaker/      # Circuit breaker usage
│   ├── dlq/                  # Dead letter queue handling
│   ├── publisher_confirms/   # Guaranteed delivery
│   └── retry/                # Retry behavior
│
├── docs/                      # Documentation
│   ├── CONFIGURATION.md      # All config options explained
│   ├── BATCH_PUBLISHING.md   # Batch publishing strategies
│   ├── CIRCUIT_BREAKER.md    # Circuit breaker guide
│   └── DLQ.md                # DLQ setup and monitoring
│
├── event.go                   # Event interface and BaseEvent
├── eventbus.go                # EventBus - main public API
├── logger.go                  # Logger interface
├── Makefile                   # Build and test commands
├── .golangci.yml             # Linter configuration
├── .pre-commit-config.yaml   # Pre-commit hooks
├── .coverignore              # Coverage exclusions
└── README.md                  # Main documentation
```

### Key Responsibilities by Package

| Package | Responsibility | Public/Internal |
|---------|---------------|-----------------|
| **Root** (`rabbitmq`) | Public API: `EventBus`, `Event`, `BaseEvent` | Public |
| `config` | Configuration types, functional options, validation | Public |
| `errors` | Custom error types, sentinel errors | Public |
| `router` | Event routing, message context, handlers | Public |
| `internal/broker` | Low-level AMQP operations (not exported) | Internal |
| `internal/circuitbreaker` | Circuit breaker state machine | Internal |
| `internal/logger` | Default logger implementation | Internal |
| `testing` | Testcontainer helpers for integration tests | Public (helper) |
| `examples` | Complete working examples | Documentation |

---

## 3. Architecture Decisions

### Architectural Pattern: **Clean Architecture with Facade Pattern**

The library follows Clean Architecture principles with clear layer separation:

```
┌─────────────────────────────────────────────────┐
│  Public API Layer (EventBus, Event, Config)    │ ← Users interact here
├─────────────────────────────────────────────────┤
│  Router Layer (Handler routing by event type)  │
├─────────────────────────────────────────────────┤
│  Internal/Broker Layer (AMQP operations)       │ ← Hidden from users
├─────────────────────────────────────────────────┤
│  RabbitMQ (amqp091-go library)                 │
└─────────────────────────────────────────────────┘
```

### Layer Separation

**1. Public API Layer** (`eventbus.go`, `event.go`)
- **Responsibility**: Provide simple, high-level interface for users
- **Pattern**: Facade pattern - hides complexity of broker layer
- **Key Types**:
  - `EventBus`: Main entry point, manages lifecycle
  - `Event`: Interface for domain events
  - `BaseEvent`: Embeddable base implementation

**2. Router Layer** (`router/`)
- **Responsibility**: Route messages to handlers based on event type
- **Pattern**: Strategy pattern for handlers
- **Key Types**:
  - `Router`: Maps event types to handlers
  - `MessageContext`: Wraps AMQP delivery with helpers
  - `HandlerService`: Interface for message handlers

**3. Internal Broker Layer** (`internal/broker/`)
- **Responsibility**: Low-level RabbitMQ operations
- **Pattern**: Repository pattern for AMQP operations
- **Key Types**:
  - `Client`: Connection/channel management
  - `Publisher`: Message publishing with confirms
  - `Consumer`: Message consumption with workers
  - `RetryHandler`: Retry logic with exponential backoff

**4. Supporting Layers**
- **Circuit Breaker** (`internal/circuitbreaker/`): State machine pattern
- **Logger** (`internal/logger/`): Adapter pattern for logging
- **Config** (`config/`): Functional options pattern

### Dependency Injection Approach

The library uses **Functional Options Pattern** for dependency injection:

```go
// Configuration is injected via functional options
type Option func(*Config)

// Example options
func WithURI(uri string) Option { ... }
func WithPrefetchCount(count int) Option { ... }
func WithLogger(logger Logger) Option { ... }

// Usage
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),          // Base config
    config.WithURI("amqp://..."),    // Inject URI
    config.WithLogger(myLogger),     // Inject custom logger
)
```

**Why Functional Options?**
- ✅ Backward compatible when adding new options
- ✅ Explicit and self-documenting
- ✅ Type-safe with compile-time checking
- ✅ Optional parameters without method overloading
- ✅ Sensible defaults via `DefaultConfig()`

### Communication Patterns

**Between Modules**:
- `EventBus` → `broker.Publisher`: Direct method calls
- `EventBus` → `broker.Consumer`: Direct method calls
- `Consumer` → `Router`: Handler lookup by event type
- `Router` → `HandlerService`: Execute method call

**Concurrency**:
- **Publisher Confirms**: Goroutine per publish with channel-based confirmation
- **Consumer Workers**: Worker pool pattern (configurable workers per queue)
- **Circuit Breaker**: Mutex-protected state machine
- **Reconnection**: Background goroutine monitoring connection

**Error Propagation**:
- Bottom-up: Internal layers return typed errors
- Wrapping: Each layer wraps errors with context
- Sentinel: Top-level sentinel errors for common cases

---

## 4. Design Patterns Implemented

### 4.1 Facade Pattern
**Where**: `eventbus.go` - `EventBus` type

**Why**: Hide complexity of multiple internal components (`Client`, `Publisher`, `Consumer`, `Router`) behind a single, simple interface.

**Implementation**:
```go
// EventBus wraps multiple internal components
type EventBus struct {
    client    *broker.Client      // Connection management
    publisher *broker.Publisher   // Publishing logic
    consumer  *broker.Consumer    // Consumption logic
    router    *router.Router      // Event routing
    dlqRouter *router.Router      // DLQ routing
}

// Users interact with simple methods
func (eb *EventBus) Publish(ctx context.Context, event Event) error
func (eb *EventBus) RegisterHandler(eventType string, handler HandlerService)
func (eb *EventBus) StartConsume(queueName string, workers int) error
```

### 4.2 Functional Options Pattern
**Where**: `config/config.go` - All `With*` functions

**Why**: Provide flexible, backward-compatible configuration without breaking API changes.

**Implementation**:
```go
type Option func(*Config)

func WithURI(uri string) Option {
    return func(c *Config) {
        c.URI = uri
    }
}

// Users chain options
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI("amqp://..."),
    config.WithPrefetchCount(10),
    config.WithCircuitBreaker(true, 5, 60*time.Second, 3),
)
```

### 4.3 Strategy Pattern
**Where**: `router/router.go` - `HandlerService` interface

**Why**: Allow users to define custom message processing strategies without modifying library code.

**Implementation**:
```go
// Strategy interface
type HandlerService interface {
    Execute(ctx *MessageContext) error
}

// Router uses strategies
type Router struct {
    handlers map[string]HandlerService
}

// Users provide strategies
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    // Custom processing logic
    return nil
})
```

### 4.4 State Machine Pattern
**Where**: `internal/circuitbreaker/circuitbreaker.go` - Circuit breaker states

**Why**: Model circuit breaker behavior with clear state transitions.

**States**:
```go
const (
    StateClosed   State = iota  // Normal operation
    StateOpen                    // Blocking requests
    StateHalfOpen                // Testing recovery
)
```

**Transitions**:
- `Closed` → `Open`: After `MaxFailures` consecutive failures
- `Open` → `HalfOpen`: After `ResetTimeout` duration
- `HalfOpen` → `Closed`: After `HalfOpenMaxRequests` successes
- `HalfOpen` → `Open`: On any failure

### 4.5 Worker Pool Pattern
**Where**: `internal/broker/consumer.go` - Message consumption

**Why**: Process messages concurrently while controlling resource usage.

**Implementation**:
```go
func (c *Consumer) Consume(queueName string, workers int) error {
    deliveries := // ... channel of messages

    // Spawn worker pool
    for i := 0; i < workers; i++ {
        go func(workerID int) {
            for delivery := range deliveries {
                // Process message
                handler.Execute(messageContext)
            }
        }(i)
    }
}
```

### 4.6 Retry Pattern with Exponential Backoff
**Where**: `internal/broker/retry.go`

**Why**: Handle transient failures gracefully without overwhelming downstream services.

**Implementation**:
```go
func (r *RetryHandler) Retry(ctx context.Context, delivery amqp.Delivery) error {
    retryCount := getRetryCount(delivery)

    if retryCount >= r.maxRetries {
        return ErrMaxRetriesExceeded
    }

    // Exponential backoff: 1s, 2s, 4s, 8s...
    delay := time.Duration(1<<retryCount) * time.Second

    // Publish with delay to retry queue
    r.publisher.Publish(ctx, exchange, routingKey, body, delay)
}
```

### 4.7 Builder Pattern (Implicit)
**Where**: `event.go` - `BaseEvent` construction

**Why**: Provide flexible event creation with required and optional fields.

**Implementation**:
```go
// Required fields
event := NewBaseEvent("exchange", "type")

// With custom ID
event := NewBaseEventWithID("custom-id", "exchange", "type")

// Can modify after creation
event.SetID("new-id")
```

### 4.8 Repository Pattern (Implicit)
**Where**: `internal/broker/client.go`

**Why**: Abstract AMQP connection/channel management from business logic.

**Implementation**:
```go
type Client struct {
    conn    *amqp.Connection
    channel *amqp.Channel
    // ... state management
}

func (c *Client) GetChannel() (*amqp.Channel, error) {
    // Manages lifecycle and reconnection
}
```

---

## 5. Coding Style & Conventions

### Naming Conventions

**Files**:
- Lowercase with underscores: `circuit_breaker.go`, `default_logger.go`
- Test files: `*_test.go`
- Package name matches directory name

**Variables & Functions**:
```go
// Exported (PascalCase)
type EventBus struct { ... }
func NewEventBus(...) (*EventBus, error)

// Unexported (camelCase)
var maxRetries int
func (c *Client) connect() error

// Constants (PascalCase or UPPERCASE_WITH_UNDERSCORES for sentinel values)
const CountHeader = "x-retry-count"
const (
    StateClosed State = iota
    StateOpen
)

// Acronyms keep casing
type DLQConfig struct { ... }  // Not DlqConfig
func (eb *EventBus) StartConsumeDLQ()  // Not StartConsumeDlq
```

**Types**:
```go
// Interfaces end with 'Service' or describe capability
type HandlerService interface
type Logger interface

// Errors start with 'Err' for sentinels
var ErrClientClosed = errors.New("...")
var ErrMaxRetriesExceeded = errors.New("...")

// Error types end with 'Error'
type ConfigError struct { ... }
type PublishError struct { ... }
```

### Code Organization Within Files

**Standard file structure**:
```go
// 1. Package declaration
package rabbitmq

// 2. Imports (grouped and sorted)
import (
    // Standard library
    "context"
    "time"

    // External dependencies
    "github.com/rabbitmq/amqp091-go"

    // Internal packages
    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/internal/broker"
)

// 3. Constants
const (
    DefaultTimeout = 10 * time.Second
)

// 4. Type definitions
type EventBus struct { ... }

// 5. Constructors (New*, Default*)
func NewEventBus(...) (*EventBus, error) { ... }

// 6. Methods (grouped by receiver)
func (eb *EventBus) Publish(...) error { ... }
func (eb *EventBus) Close() error { ... }

// 7. Helper functions (unexported)
func validateEvent(event Event) error { ... }
```

### Import Ordering

**Three groups separated by blank lines**:
1. Standard library
2. External dependencies
3. Internal packages

```go
import (
    "context"
    "encoding/json"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/google/uuid"

    "github.com/edaniel30/rabbitmq-kit-go/config"
    "github.com/edaniel30/rabbitmq-kit-go/errors"
)
```

### Comments and Documentation

**Godoc comments** (full sentences):
```go
// EventBus provides a high-level interface for publishing domain events.
//
// The EventBus is designed to work with the Event interface and follows
// the Domain-Driven Design pattern.
//
// Example usage:
//
//  eventBus, _ := rabbitmq.NewEventBus(
//      config.DefaultConfig(),
//      config.WithURI("amqp://localhost:5672/"),
//  )
//  defer eventBus.Close()
type EventBus struct { ... }
```

**Implementation comments** (explain why, not what):
```go
// Circuit is open, reject message without processing
if !cb.AllowRequest() {
    return errors.New("circuit breaker open")
}
```

**TODO comments** (include issue number if applicable):
```go
// TODO: Add support for delayed exchanges
// TODO(#42): Implement connection pooling
```

### Linting/Formatting Rules

**Enforced by `.golangci.yml`**:
- `errcheck`: All errors must be checked or explicitly ignored with `_ =`
- `govet`: Standard Go vet checks
- `staticcheck`: Static analysis
- `unused`: No unused variables/imports
- `gocritic`: Style and performance checks
- `exhaustive`: Switch statements must handle all enum values
- `errorlint`: Use `errors.Is()`/`errors.As()` for wrapped errors

**Formatting**:
```bash
# Run before commit
gofmt -w .
go mod tidy
golangci-lint run

# Or use pre-commit hooks
make pre-commit
```

**Test exclusions**:
- `*_test.go` files: Relaxed `errcheck`, `gocritic`, `unparam`
- `testing/` package: Relaxed `errcheck`

---

## 6. Error Handling Strategy

### Error Types Hierarchy

```
error (interface)
├── Sentinel Errors (package-level vars)
│   ├── ErrClientClosed
│   ├── ErrNoConnection
│   ├── ErrNoChannel
│   ├── ErrMaxRetriesExceeded
│   ├── ErrNoHandlersRegistered
│   ├── ErrPublishNotConfirmed
│   └── ErrPublishConfirmTimeout
│
└── Typed Errors (structs with context)
    ├── ConfigError
    ├── ConnectionError
    ├── PublishError
    ├── ConsumeError
    ├── TopologyError
    └── HandlerError
```

### When to Use Each Error Type

**Sentinel Errors**: For well-known error conditions that users should check for
```go
import "github.com/edaniel30/rabbitmq-kit-go/errors"

err := eventBus.Publish(ctx, event)
if errors.Is(err, errors.ErrPublishNotConfirmed) {
    // Handle NACK from broker
}
```

**Typed Errors**: For errors that need additional context
```go
// Library creates typed errors internally
if err != nil {
    return errors.NewPublishError(exchange, routingKey, err)
}

// Users can extract context
var pubErr *errors.PublishError
if errors.As(err, &pubErr) {
    log.Printf("Failed to publish to %s with key %s",
        pubErr.Exchange, pubErr.RoutingKey)
}
```

### Error Propagation Pattern

**Bottom-up with wrapping**:
```go
// Low level (internal/broker/publisher.go)
func (p *Publisher) publish(...) error {
    if err := channel.Publish(...); err != nil {
        return errors.NewPublishError(exchange, routingKey, err)
    }
    return nil
}

// Mid level (eventbus.go)
func (eb *EventBus) Publish(ctx context.Context, event Event) error {
    if err := eb.publisher.Publish(...); err != nil {
        eb.config.Logger.Error(ctx, "Failed to publish event", map[string]any{
            "event_type": event.Type(),
            "error":      err,
        })
        return err  // Propagate with context preserved
    }
    return nil
}
```

### Logging Approach

**Structured logging with context**:
```go
type Logger interface {
    Info(ctx context.Context, msg string, fields map[string]any)
    Error(ctx context.Context, msg string, fields map[string]any)
    Warn(ctx context.Context, msg string, fields map[string]any)
    Debug(ctx context.Context, msg string, fields map[string]any)
    Close() error
}

// Usage
logger.Error(ctx, "Consumer Worker: Handler error", map[string]any{
    "worker_id": workerID,
    "event_type": eventType,
    "error": err,
    "retry_count": retryCount,
})
```

**Log levels**:
- **Debug**: Development/troubleshooting (verbose)
- **Info**: Normal operations (connection established, confirms enabled)
- **Warn**: Degraded operation (circuit breaker open, max retries exceeded)
- **Error**: Failures (connection lost, publish failed, handler error)

**Default logger**: Writes to stdout (Info, Debug) and stderr (Warn, Error) with timestamp and structured fields.

### Error Response Format

**Not applicable** - This is a library, not an API. Errors are returned as Go error values.

**Error checking pattern for users**:
```go
import stderrors "errors"
import "github.com/edaniel30/rabbitmq-kit-go/errors"

// Check sentinel errors
if stderrors.Is(err, errors.ErrPublishNotConfirmed) { ... }

// Check typed errors
var connErr *errors.ConnectionError
if stderrors.As(err, &connErr) {
    log.Printf("Connection failed during: %s", connErr.Operation)
}

// Generic error handling
if err != nil {
    log.Printf("Operation failed: %v", err)
}
```

---

## 7. Data Flow

### Request/Response Lifecycle - Publishing

```
User Code
  │
  │ event := &UserCreatedEvent{...}
  │ err := eventBus.Publish(ctx, event)
  ▼
EventBus.Publish()
  │
  │ 1. Convert Event to JSON
  │ 2. Extract exchange and routing key
  ▼
broker.Publisher.Publish()
  │
  │ 3. Check if client is closed
  │ 4. Get AMQP channel
  │ 5. Prepare AMQP message
  ▼
amqp.Channel.Publish()
  │
  │ 6. Send to RabbitMQ
  ▼
[Publisher Confirms enabled?]
  │
  ├─ Yes → Wait for confirm with timeout
  │         ├─ ACK → Return nil
  │         ├─ NACK → Return ErrPublishNotConfirmed
  │         └─ Timeout → Return ErrPublishConfirmTimeout
  │
  └─ No → Return nil immediately
```

### Request/Response Lifecycle - Consuming

```
RabbitMQ Delivery
  │
  ▼
broker.Consumer.Consume() [Worker Pool]
  │
  │ For each worker (goroutine):
  │   1. Receive delivery from channel
  │   2. Wrap in MessageContext
  │   3. Check circuit breaker
  │   4. Extract event type from JSON
  ▼
router.Router.GetHandler()
  │
  │ 5. Lookup handler by event type
  ▼
HandlerService.Execute(ctx)
  │
  │ 6. User handler processes message
  │
  ├─ Return nil
  │   ├─ Success → ACK message
  │   └─ Record success in circuit breaker
  │
  └─ Return error
      ├─ Get retry count from headers
      │
      ├─ retryCount < maxRetries
      │   ├─ Increment x-retry-count
      │   ├─ Publish to retry queue with delay
      │   ├─ ACK original message
      │   └─ Record failure in circuit breaker
      │
      └─ retryCount >= maxRetries
          ├─ NACK without requeue (→ DLX if configured)
          └─ Log "Max retries exceeded"
```

### Data Validation Approach

**Configuration validation** (at startup):
```go
func (c *Config) Validate() error {
    if c.URI == "" {
        return errors.NewConfigFieldError("URI", "is required")
    }

    if c.PrefetchCount < 0 {
        return errors.NewConfigFieldError("PrefetchCount", "cannot be negative")
    }

    // Validate exchange types
    validTypes := map[string]bool{
        "direct": true, "fanout": true, "topic": true, "headers": true,
    }
    if !validTypes[ex.Type] {
        return errors.NewConfigFieldError(...)
    }

    return nil
}
```

**Event validation** (implicit via interface):
```go
// Events must implement this interface
type Event interface {
    Type() string           // Cannot be empty
    Exchange() string       // Cannot be empty
    ToMap() map[string]any  // Must be JSON-serializable
}

// Validation happens at publish time
func (eb *EventBus) Publish(ctx context.Context, event Event) error {
    // 1. Call ToMap() - will panic if implementation is broken
    data := event.ToMap()

    // 2. Marshal to JSON - returns error if not serializable
    body, err := json.Marshal(data)
    if err != nil {
        return errors.NewPublishError(event.Exchange(), event.Type(), err)
    }

    // 3. Validate exchange and routing key are non-empty
    if event.Exchange() == "" || event.Type() == "" {
        return errors.New("event exchange and type cannot be empty")
    }
}
```

**Message validation** (at consumption):
```go
func (c *MessageContext) GetType() string {
    var payload struct {
        Type string `json:"type"`
    }

    // If JSON parsing fails, return empty string
    // This causes router to not find handler and ACK the message
    if err := json.Unmarshal(c.Delivery.Body, &payload); err != nil {
        return ""
    }

    return payload.Type
}
```

### DTOs, Entities, and Domain Models Transformation

**Event (Domain Model)** → **JSON (DTO)** → **AMQP Message (Transport)**

**1. Domain Event → JSON DTO**:
```go
// Domain model
type UserCreatedEvent struct {
    *BaseEvent
    UserID   string
    Username string
    Email    string
}

// Convert to DTO (map)
func (e *UserCreatedEvent) ToMap() map[string]any {
    m := e.BaseEvent.ToMap()  // id, occurred_at, type
    m["user_id"] = e.UserID
    m["username"] = e.Username
    m["email"] = e.Email
    return m
}

// Serialize to JSON
data := event.ToMap()
body, _ := json.Marshal(data)
```

**2. JSON → AMQP Message**:
```go
// Add metadata as AMQP headers
msg := amqp.Publishing{
    ContentType:  "application/json",
    Body:         body,
    DeliveryMode: 2,  // Persistent
    Timestamp:    time.Now(),
    MessageId:    uuid.New().String(),
}
```

**3. AMQP Message → JSON DTO → Domain Model (Consumer side)**:
```go
// Wrapped in MessageContext
func handler(ctx *MessageContext) error {
    // Deserialize to domain model
    var user UserCreatedEvent
    if err := ctx.BindJSON(&user); err != nil {
        return err
    }

    // Use domain model
    processUser(user)
    return nil
}
```

**Transformation rules**:
- ✅ Always include `"type"` field in JSON for routing
- ✅ Use `BaseEvent` to add common fields (id, occurred_at)
- ✅ Keep domain logic in domain events, not in serialization
- ✅ Make ToMap() idempotent - can be called multiple times
- ❌ Don't put business logic in ToMap()
- ❌ Don't modify event state in ToMap()

---

## 8. Testing Strategy

### Test Types

**Unit Tests** (no Docker required):
- Package: `config`, `errors`, `router`, `internal/circuitbreaker`, `internal/logger`
- Run with: `make test-unit` or `go test -short ./...`
- Fast (<1 second total)

**Integration Tests** (require Docker):
- Package: `internal/broker`, root package (`eventbus_test.go`)
- Use testcontainers to spin up RabbitMQ
- Run with: `make test` or `go test ./...`
- Slower (~30 seconds total)

### Testing Frameworks and Tools

```go
import (
    "testing"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
    rabbitTest "github.com/edaniel30/rabbitmq-kit-go/testing"
)
```

**testify/assert**: Soft assertions (test continues)
```go
assert.Equal(t, expected, actual)
assert.NoError(t, err)
assert.True(t, condition)
```

**testify/require**: Hard assertions (test stops)
```go
require.NoError(t, err)  // Stop if error
require.NotNil(t, obj)   // Stop if nil
```

**testcontainers**: Docker-based integration testing
```go
func TestMain(m *testing.M) {
    container := rabbitTest.SetupRabbitMQContainer(&testing.T{})
    code := m.Run()
    container.Teardown(&testing.T{})
    os.Exit(code)
}
```

### How to Run Tests

```bash
# All tests (requires Docker)
make test

# Unit tests only (fast, no Docker)
make test-unit

# Integration tests only
go test -v -run Integration ./...

# With coverage
make test-coverage

# Coverage threshold check (fails if < 80%)
make test-coverage  # Checks threshold automatically

# HTML coverage report
make test-coverage-html
```

### Test Organization Patterns

**1. Table-Driven Tests** (preferred for multiple scenarios):
```go
func TestPublishBatch_Variants(t *testing.T) {
    tests := []struct {
        name            string
        events          []Event
        options         []config.BatchOption
        expectSuccess   int
        expectFailed    int
        expectError     bool
    }{
        {
            name: "pipelining mode",
            events: []Event{...},
            options: []config.BatchOption{config.WithPipelining(true)},
            expectSuccess: 3,
        },
        {
            name: "sequential mode",
            events: []Event{...},
            options: []config.BatchOption{config.WithPipelining(false)},
            expectSuccess: 2,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := eventBus.PublishBatch(ctx, tt.events, tt.options...)
            assert.Equal(t, tt.expectSuccess, result.Success)
        })
    }
}
```

**2. Setup Functions** (avoid duplication):
```go
func setupConfig() config.Config {
    cfg := config.DefaultConfig()
    cfg.URI = sharedContainer.URI
    cfg.Exchanges = []config.ExchangeConfig{
        {Name: "test.exchange", Type: "topic", Durable: true},
    }
    return cfg
}

func TestSomething(t *testing.T) {
    cfg := setupConfig()
    eb, err := NewEventBus(cfg)
    // ...
}
```

**3. TestMain for Shared Setup**:
```go
var sharedContainer *rabbitTest.RabbitMQContainer

func TestMain(m *testing.M) {
    sharedContainer = rabbitTest.SetupRabbitMQContainer(&testing.T{})
    code := m.Run()
    sharedContainer.Teardown(&testing.T{})
    os.Exit(code)
}
```

### Mocking Strategies

**1. Interface Mocking** (for external dependencies):
```go
// Mock AMQP Acknowledger
type mockAcknowledger struct {
    acked    bool
    nacked   bool
    requeued bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
    m.acked = true
    return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
    m.nacked = true
    m.requeued = requeue
    return nil
}

// Usage in test
ctx := &MessageContext{
    Delivery: amqp.Delivery{
        Acknowledger: &mockAcknowledger{},
    },
}
err := ctx.Ack()
assert.NoError(t, err)
assert.True(t, ctx.Delivery.Acknowledger.(*mockAcknowledger).acked)
```

**2. Test Doubles** (for handlers):
```go
type testEventHandler struct {
    mu       sync.Mutex
    messages []string
    count    int
    err      error  // Inject errors
}

func (h *testEventHandler) Execute(ctx *router.MessageContext) error {
    h.mu.Lock()
    defer h.mu.Unlock()
    h.count++
    return h.err  // Return injected error
}
```

**3. Real RabbitMQ** (integration tests):
- Use testcontainers to spin up real RabbitMQ
- No mocking of broker layer
- Tests real AMQP behavior

### Coverage Exclusions

See `.coverignore`:
```
/examples     # Example code not counted
/testing      # Test utilities not counted
/internal/mocks  # Mock implementations not counted
```

Current coverage: **81.4%** (threshold: 80%)

---

## 9. External Integrations

### Third-Party Services and APIs

**RabbitMQ** (`github.com/rabbitmq/amqp091-go`):
- **Purpose**: Message broker for event-driven architecture
- **Operations**: Publish, consume, topology management, publisher confirms
- **Connection management**: Auto-reconnection with exponential backoff

### How Integrations Are Abstracted

**1. Broker Layer Abstraction** (`internal/broker/`)

```go
// internal/broker/client.go
type Client struct {
    conn    *amqp.Connection  // amqp091-go connection
    channel *amqp.Channel     // amqp091-go channel
    // ... state management
}

// Public methods hide AMQP details
func (c *Client) GetChannel() (*amqp.Channel, error)
func (c *Client) IsConnected() bool
func (c *Client) Close() error
```

**Why?**
- ✅ Encapsulates reconnection logic
- ✅ Hides AMQP channel/connection lifecycle
- ✅ Could swap AMQP implementation without affecting users
- ✅ Easier to test - can mock `Client` interface

**2. Logger Abstraction** (`logger.go`)

```go
// Public interface (in root package)
type Logger interface {
    Info(ctx context.Context, msg string, fields map[string]any)
    Error(ctx context.Context, msg string, fields map[string]any)
    Warn(ctx context.Context, msg string, fields map[string]any)
    Debug(ctx context.Context, msg string, fields map[string]any)
    Close() error
}

// Default implementation (in internal/logger/)
type DefaultLogger struct {
    infoLogger  *log.Logger
    errorLogger *log.Logger
}
```

**Why?**
- ✅ Users can inject custom loggers (Zap, Logrus, etc.)
- ✅ Library doesn't force logging framework choice
- ✅ Default implementation for quick start

**Usage**:
```go
// Use default logger
eventBus, _ := rabbitmq.NewEventBus(config.DefaultConfig())

// Or inject custom logger
eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithLogger(myZapLogger),
)
```

**3. Circuit Breaker** (no external dependency)

Implemented entirely within library - no external circuit breaker library.

**Why?**
- ✅ Full control over behavior
- ✅ No external dependencies to manage
- ✅ Optimized for RabbitMQ use case

### Configuration Management

**Functional Options Pattern**:
```go
// All configuration via functional options
eventBus, err := rabbitmq.NewEventBus(
    config.DefaultConfig(),  // Base defaults
    config.WithURI(os.Getenv("RABBITMQ_URI")),  // From environment
    config.WithPrefetchCount(50),
    config.WithCircuitBreaker(
        true,
        5,                    // maxFailures
        60*time.Second,       // resetTimeout
        3,                    // halfOpenRequests
    ),
)
```

**Environment variable usage** (application-level):
```go
// Application reads env vars, passes to library
rabbitURI := os.Getenv("RABBITMQ_URI")
if rabbitURI == "" {
    rabbitURI = "amqp://guest:guest@localhost:5672/"
}

eventBus, _ := rabbitmq.NewEventBus(
    config.DefaultConfig(),
    config.WithURI(rabbitURI),
)
```

**No magic environment variable reading** - library doesn't read `os.Getenv()` itself for clarity and testability.

---

## 10. Important Conventions & Rules

### Business Rules

**1. Event Type Routing**:
- ✅ Events MUST have a `"type"` field in JSON for routing
- ✅ Event type used as routing key
- ❌ Don't use event type as exchange name

**2. Retry Behavior**:
- After handler returns error:
  1. Check retry count in `x-retry-count` header
  2. If `retryCount < maxRetries`: Requeue with incremented count
  3. If `retryCount >= maxRetries`: Send to DLX (if configured) or discard
- Exponential backoff: 1s, 2s, 4s, 8s...
- Max retries default: 3

**3. Circuit Breaker Behavior**:
- Only applies to **consumers**, not publishers
- Tracks failures per consumer instance (not global)
- On open: Messages are NACK'd without requeue → DLX

**4. Publisher Confirms**:
- If enabled, `Publish()` blocks until confirmed or timeout
- Timeout default: 5 seconds
- NACK returns `ErrPublishNotConfirmed`
- Timeout returns `ErrPublishConfirmTimeout`

**5. DLQ Convention**:
- DLX exchange: `dlx.exchange` or custom prefix
- DLQ queue: `dlq.{original_queue_name}`
- Routing: Same as original queue

### Security Considerations

**1. Connection URI Security**:
```go
// ❌ Don't hardcode credentials
config.WithURI("amqp://admin:password123@prod:5672/")

// ✅ Use environment variables
config.WithURI(os.Getenv("RABBITMQ_URI"))

// ✅ Use secrets management
config.WithURI(secretsManager.GetString("rabbitmq-uri"))
```

**2. Message Security**:
- Library does NOT encrypt message bodies
- If needed, encrypt before calling `Publish()`
- Consider RabbitMQ TLS connections: `amqps://...`

**3. Error Messages**:
- Don't log sensitive data in error messages
- Error types don't expose credentials

### Performance Considerations

**1. Prefetch Count** (critical tuning):
```go
// Fast processing (<100ms): 50-100
config.WithPrefetchCount(100)

// Medium (100ms-1s): 10-20
config.WithPrefetchCount(10)

// Slow (>1s): 1-5
config.WithPrefetchCount(1)
```

**2. Worker Count**:
```go
// CPU-bound: workers = num_cores
eventBus.StartConsume("queue", runtime.NumCPU())

// I/O-bound: workers = 2-4x num_cores
eventBus.StartConsume("queue", runtime.NumCPU() * 3)

// Very slow (DB writes): workers = 1-2 per core
eventBus.StartConsume("queue", runtime.NumCPU())
```

**3. Batch Publishing**:
```go
// Pipelined: 5-10x faster than sequential
result, _ := eventBus.PublishBatch(ctx, events,
    config.WithPipelining(true),
)

// Async: Fastest for large batches (1000+)
result, _ := eventBus.PublishBatchAsync(ctx, events,
    config.WithMaxConcurrency(50),
)
```

**4. Connection Pooling**:
- One `EventBus` per application recommended
- Reuse across goroutines - thread-safe
- Don't create per-request

### Anti-Patterns to Avoid

**❌ Don't create EventBus per request**:
```go
// ❌ BAD: Creates new connection per request
func HandleRequest(w http.ResponseWriter, r *http.Request) {
    eb, _ := rabbitmq.NewEventBus(...)  // Don't do this!
    defer eb.Close()
    eb.Publish(ctx, event)
}

// ✅ GOOD: Reuse EventBus
var globalEventBus *rabbitmq.EventBus

func init() {
    globalEventBus, _ = rabbitmq.NewEventBus(...)
}

func HandleRequest(w http.ResponseWriter, r *http.Request) {
    globalEventBus.Publish(ctx, event)  // Reuse connection
}
```

**❌ Don't block in handlers**:
```go
// ❌ BAD: Blocking I/O in handler
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    time.Sleep(10 * time.Second)  // Blocks worker!
    return nil
})

// ✅ GOOD: Quick processing or async
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    go processAsync(ctx)  // Process in background
    return nil  // ACK immediately
})
```

**❌ Don't ignore context**:
```go
// ❌ BAD: No timeout
err := eventBus.Publish(context.Background(), event)

// ✅ GOOD: Use timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
err := eventBus.Publish(ctx, event)
```

**❌ Don't panic in handlers**:
```go
// ❌ BAD: Panic crashes worker
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    panic("something went wrong")  // Crashes goroutine!
})

// ✅ GOOD: Return error
eventBus.RegisterHandler("user.created", func(ctx *MessageContext) error {
    if err := validate(ctx); err != nil {
        return err  // Will retry with backoff
    }
    return nil
})
```

**❌ Don't mutate events after publish**:
```go
// ❌ BAD: Event modified after publish
event := &UserCreatedEvent{...}
eventBus.Publish(ctx, event)
event.UserID = "different-id"  // Race condition if ToMap() called async

// ✅ GOOD: Treat events as immutable
event := &UserCreatedEvent{...}
eventBus.Publish(ctx, event)
// Don't modify event after publish
```

---

## 11. Common Commands

### Build, Run, Test

```bash
# Build (optional - Go builds on demand)
go build ./...

# Run example
export RABBITMQ_URI="amqp://guest:guest@localhost:5672/"
go run examples/basic/main.go

# Run all tests (requires Docker)
make test

# Run unit tests only (fast, no Docker)
make test-unit
# OR
go test -short ./...

# Run integration tests only
go test -v -run Integration ./...

# Run specific test
go test -v -run TestEventBus_Integration ./...

# Run with race detector
make test-race
# OR
go test -race ./...
```

### Coverage

```bash
# Generate coverage report
make test-coverage

# View HTML coverage report
make test-coverage-html

# Check coverage meets threshold (80%)
make test-coverage  # Fails if below threshold
```

### Linting

```bash
# Run all pre-commit checks
make pre-commit

# Run linter manually
golangci-lint run

# Fix auto-fixable issues
golangci-lint run --fix

# Format code
gofmt -w .

# Tidy dependencies
go mod tidy
```

### Pre-commit Hooks

```bash
# Install hooks (one time)
pip install pre-commit
make setup

# Run manually
pre-commit run --all-files

# Disable for one commit (not recommended)
git commit --no-verify
```

### Cleanup

```bash
# Remove coverage files
make clean

# Remove testcontainer artifacts
docker ps -a | grep testcontainers | awk '{print $1}' | xargs docker rm
```

---

## 12. Future Development Guidelines

### How to Add New Features

**1. Adding a New Configuration Option**

```go
// Step 1: Add field to Config struct (config/config.go)
type Config struct {
    // ... existing fields
    NewFeatureEnabled bool          // Enable new feature
    NewFeatureTimeout time.Duration // Timeout for new feature
}

// Step 2: Update DefaultConfig
func DefaultConfig() Config {
    return Config{
        // ... existing defaults
        NewFeatureEnabled: false,
        NewFeatureTimeout: 30 * time.Second,
    }
}

// Step 3: Add functional option
func WithNewFeature(enabled bool, timeout time.Duration) Option {
    return func(c *Config) {
        c.NewFeatureEnabled = enabled
        c.NewFeatureTimeout = timeout
    }
}

// Step 4: Add validation if needed
func (c *Config) Validate() error {
    // ... existing validation
    if c.NewFeatureEnabled && c.NewFeatureTimeout <= 0 {
        return errors.NewConfigFieldError("NewFeatureTimeout", "must be > 0 when enabled")
    }
    return nil
}

// Step 5: Add tests (config/config_test.go)
func TestWithNewFeature(t *testing.T) { ... }
```

**2. Adding a New Event Type**

```go
// Step 1: Define event struct
type OrderCompletedEvent struct {
    *rabbitmq.BaseEvent
    OrderID     string
    TotalAmount float64
    CompletedAt time.Time
}

// Step 2: Implement ToMap()
func (e *OrderCompletedEvent) ToMap() map[string]any {
    m := e.BaseEvent.ToMap()
    m["order_id"] = e.OrderID
    m["total_amount"] = e.TotalAmount
    m["completed_at"] = e.CompletedAt
    return m
}

// Step 3: Constructor (optional)
func NewOrderCompletedEvent(orderID string, amount float64) *OrderCompletedEvent {
    return &OrderCompletedEvent{
        BaseEvent:   rabbitmq.NewBaseEvent("orders.exchange", "order.completed"),
        OrderID:     orderID,
        TotalAmount: amount,
        CompletedAt: time.Now(),
    }
}

// Step 4: Register handler
eventBus.RegisterHandler("order.completed", func(ctx *router.MessageContext) error {
    var evt OrderCompletedEvent
    if err := ctx.BindJSON(&evt); err != nil {
        return err
    }
    // Process order...
    return nil
})
```

**3. Adding New Error Type**

```go
// Step 1: Define error struct (errors/errors.go)
type BatchError struct {
    Operation string
    EventsTotal int
    EventsFailed int
    Errors []error
}

func (e *BatchError) Error() string {
    return fmt.Sprintf("rabbitmq: batch operation '%s' failed: %d/%d events failed",
        e.Operation, e.EventsFailed, e.EventsTotal)
}

func (e *BatchError) Unwrap() error {
    if len(e.Errors) == 1 {
        return e.Errors[0]
    }
    return nil
}

// Step 2: Constructor
func NewBatchError(operation string, total, failed int, errs []error) error {
    return &BatchError{
        Operation: operation,
        EventsTotal: total,
        EventsFailed: failed,
        Errors: errs,
    }
}

// Step 3: Add tests
func TestBatchError(t *testing.T) { ... }
```

### Checklist for New Public Methods

When adding a new public method to `EventBus`:

- [ ] Add godoc comment with description and example
- [ ] Check if context parameter is needed
- [ ] Return typed error (not generic error)
- [ ] Add logging for error cases
- [ ] Implement in `internal/broker` layer first
- [ ] Write unit tests
- [ ] Write integration test (if interacts with RabbitMQ)
- [ ] Update README.md if user-facing
- [ ] Add example to `examples/` if complex
- [ ] Update documentation in `docs/` if needed

### Checklist for New Internal Packages

When adding a new package under `internal/`:

- [ ] Keep package focused on single responsibility
- [ ] Don't export types unless needed by other internal packages
- [ ] Add package-level godoc comment
- [ ] Write unit tests (aim for >80% coverage)
- [ ] Use structured logging with `Logger` interface
- [ ] Return typed errors from `errors/` package
- [ ] Avoid depending on `eventbus.go` (circular dependency)

### Code Review Criteria

**Must Have**:
1. ✅ All tests pass (`make test`)
2. ✅ Coverage ≥ 80% (`make test-coverage`)
3. ✅ Linter passes (`make pre-commit`)
4. ✅ No breaking changes to public API
5. ✅ Godoc comments for public types/methods

**Should Have**:
6. ✅ Integration test for RabbitMQ interactions
7. ✅ Example in `examples/` for new features
8. ✅ Error handling with typed errors
9. ✅ Structured logging in error paths
10. ✅ Context support for cancellation

**Good to Have**:
11. ✅ Benchmarks for performance-critical code
12. ✅ Documentation in `docs/` for complex features
13. ✅ Backward compatibility path for config changes

### Adding Tests for Existing Code

**Pattern to follow**:
```go
func TestNewFeature(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")  // For integration tests
    }

    // Setup
    cfg := config.DefaultConfig()
    cfg.URI = sharedContainer.URI
    eb, err := NewEventBus(cfg)
    require.NoError(t, err)
    defer func() { _ = eb.Close() }()

    // Table-driven tests for multiple scenarios
    tests := []struct {
        name        string
        input       Input
        expected    Expected
        expectError bool
    }{
        {"success case", Input{...}, Expected{...}, false},
        {"error case", Input{...}, Expected{...}, true},
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // Test logic
            result, err := DoSomething(tt.input)

            if tt.expectError {
                assert.Error(t, err)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.expected, result)
            }
        })
    }
}
```

---

## Summary

This codebase follows **Clean Architecture** with **Facade** and **Functional Options** patterns. Key points:

1. **Simple Public API**: `EventBus` hides complexity
2. **Internal Isolation**: `internal/` packages not exported
3. **Type-Safe Config**: Functional options with validation
4. **Robust Error Handling**: Typed errors with context
5. **Production-Ready**: Retries, circuit breakers, DLQ, publisher confirms
6. **High Test Coverage**: 81.4% with integration tests
7. **Extensible**: Easy to add new features following existing patterns

**When in doubt**:
- ✅ Check `examples/` for usage patterns
- ✅ Read godoc comments for detailed behavior
- ✅ Look at existing tests for test patterns
- ✅ Use functional options for new config
- ✅ Return typed errors from `errors/` package
