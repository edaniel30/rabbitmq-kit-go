package rabbitmq

import (
	"context"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/broker"
	"github.com/edaniel30/rabbitmq-kit-go/internal/circuitbreaker"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	amqp "github.com/rabbitmq/amqp091-go"
)

// EventBus provides a high-level interface for publishing domain events.
//
// The EventBus is designed to work with the Event interface and follows
// the Domain-Driven Design pattern. It allows publishing single events
// or batches of events that have been accumulated in domain aggregates.
//
// This is the recommended way to interact with RabbitMQ for event-driven
// architectures. The EventBus manages the underlying RabbitMQ client
// automatically.
//
// Example usage for publishing events:
//
//	eventBus, _ := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithURI("amqp://localhost:5672/"),
//	)
//	defer eventBus.Close()
//
//	event := NewOrderCreatedEvent(order.ID)
//	err := eventBus.Publish(ctx, event)
//
// Example usage for consuming events:
//
//	eventBus, _ := rabbitmq.NewEventBus(config.DefaultConfig(), config.WithURI("..."))
//	defer eventBus.Close()
//
//	// Register handlers
//	eventBus.RegisterHandler("order.created", orderCreatedHandler)
//	eventBus.RegisterHandler("order.completed", orderCompletedHandler)
//
//	// Start consuming
//	eventBus.StartConsume("orders.queue", 5)
type EventBus struct {
	client    *broker.Client
	publisher *broker.Publisher
	consumer  *broker.Consumer
	router    *router.Router
	dlqRouter *router.Router // Separate router for DLQ handlers
}

// NewEventBus creates a new event bus with its own RabbitMQ client.
//
// This is the recommended way to create an EventBus. The EventBus will
// manage the connection lifecycle automatically.
//
// The router is created internally and handlers can be registered using
// RegisterHandler() method.
//
// Example:
//
//	eventBus, err := rabbitmq.NewEventBus(
//	    config.DefaultConfig(),
//	    config.WithURI("amqp://guest:guest@localhost:5672/"),
//	    config.WithExchanges([]config.ExchangeConfig{
//	        {Name: "orders.exchange", Type: "direct", Durable: true},
//	    }),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer eventBus.Close()
func NewEventBus(cfg config.Config, opts ...config.Option) (*EventBus, error) {
	// Apply functional options
	for _, opt := range opts {
		opt(&cfg)
	}

	// Set default logger if not provided
	if cfg.Logger == nil {
		cfg.Logger = logger.New()
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	brokerClient, err := broker.New(cfg)
	if err != nil {
		return nil, err
	}

	publisher := broker.NewPublisher(brokerClient)

	return &EventBus{
		client:    brokerClient,
		publisher: publisher,
		consumer:  nil,
		router:    nil,
	}, nil
}

// RegisterHandler registers a handler for a specific event type.
//
// Handlers are used when consuming messages. The event type is extracted
// from the message's "type" field in the JSON payload.
//
// Example:
//
//	eventBus.RegisterHandler("user.created", userCreatedHandler)
//	eventBus.RegisterHandler("order.completed", orderCompletedHandler)
func (b *EventBus) RegisterHandler(eventType string, handler router.HandlerService) {
	if b.router == nil {
		b.router = router.NewRouter()
	}

	b.router.Handle(eventType, handler)
}

// StartConsume starts consuming messages from a queue with multiple workers.
//
// Before calling this method, you must register handlers using RegisterHandler().
// Messages with unregistered event types will be acknowledged and discarded.
//
// Returns ErrNoHandlersRegistered if no handlers have been registered.
//
// Parameters:
//   - queue: name of the queue to consume from
//   - workers: number of concurrent workers to process messages
//
// Example:
//
//	eventBus.RegisterHandler("order.created", orderHandler)
//	err := eventBus.StartConsume("orders.queue", 5)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (b *EventBus) StartConsume(queue string, workers int) error {
	if b.router == nil {
		return errors.ErrNoHandlersRegistered
	}

	if b.consumer == nil {
		b.consumer = broker.NewConsumer(b.client, b.publisher, b.router)
	}

	return b.consumer.Consume(queue, workers)
}

// Close closes the underlying RabbitMQ client and cleans up resources.
//
// This stops the publisher's confirmation processor goroutine (if running)
// and then closes the RabbitMQ client connection.
func (b *EventBus) Close() error {
	// Stop publisher confirmation processor first
	if b.publisher != nil {
		b.publisher.Close()
	}

	// Close the client connection
	return b.client.Close()
}

// IsConnected returns true if the underlying client is connected.
func (b *EventBus) IsConnected() bool {
	return b.client.IsConnected()
}

// GetCircuitBreakerMetrics returns the current circuit breaker metrics.
//
// Returns nil if circuit breaker is not enabled or consumer is not initialized.
//
// Example:
//
//	metrics := eventBus.GetCircuitBreakerMetrics()
//	if metrics != nil {
//	    log.Printf("Circuit breaker state: %s", metrics.State)
//	    log.Printf("Failures: %d, Successes: %d", metrics.Failures, metrics.Successes)
//	}
func (b *EventBus) GetCircuitBreakerMetrics() *circuitbreaker.Metrics {
	if b.consumer == nil {
		return nil
	}
	return b.consumer.GetCircuitBreakerMetrics()
}

// ResetCircuitBreaker manually resets the circuit breaker to closed state.
//
// This should be used cautiously, typically only for manual intervention.
// Returns false if circuit breaker is not enabled or consumer is not initialized.
//
// Example:
//
//	if eventBus.ResetCircuitBreaker() {
//	    log.Println("Circuit breaker reset successfully")
//	}
func (b *EventBus) ResetCircuitBreaker() bool {
	if b.consumer == nil {
		return false
	}
	return b.consumer.ResetCircuitBreaker()
}

// RegisterDLQHandler registers a handler for processing DLQ messages of a specific event type.
//
// DLQ handlers receive messages that have failed processing in the main queue
// after all retry attempts. Use this to implement custom recovery logic,
// analysis, or alerting for failed messages.
//
// The handler receives a MessageContext which can be converted to DLQMessage
// to access metadata about why the message failed (retry count, death reason, etc.).
//
// Example:
//
//	eventBus.RegisterDLQHandler("order.created", func(ctx *router.MessageContext) error {
//	    dlqMsg := router.NewDLQMessage(ctx)
//	    log.Printf("DLQ: %s", dlqMsg.GetDeathInfo())
//
//	    // Decide whether to retry or discard
//	    if dlqMsg.ShouldRetry(10) {
//	        return eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
//	    }
//
//	    // Log and discard
//	    log.Printf("Permanently failed: %s", dlqMsg.GetDeathInfo())
//	    return nil
//	})
func (b *EventBus) RegisterDLQHandler(eventType string, handler router.HandlerService) {
	if b.dlqRouter == nil {
		b.dlqRouter = router.NewRouter()
	}
	b.dlqRouter.Handle(eventType, handler)
}

// StartConsumeDLQ starts consuming messages from a DLQ with multiple workers.
//
// Before calling this method, you must register DLQ handlers using RegisterDLQHandler().
// If no handlers are registered, returns ErrNoHandlersRegistered.
//
// The queue parameter should be the DLQ name (e.g., "dlq.orders.queue").
// If you enabled automatic DLQ setup, the DLQ name follows the pattern: dlqPrefix + queueName.
//
// Parameters:
//   - queue: name of the DLQ to consume from (e.g., "dlq.orders.queue")
//   - workers: number of concurrent workers to process DLQ messages
//
// Example:
//
//	// Register DLQ handler
//	eventBus.RegisterDLQHandler("order.created", func(ctx *router.MessageContext) error {
//	    dlqMsg := router.NewDLQMessage(ctx)
//	    log.Printf("Processing failed order: %s", dlqMsg.GetDeathInfo())
//	    // Analyze or retry logic here
//	    return nil
//	})
//
//	// Start consuming from DLQ
//	err := eventBus.StartConsumeDLQ("dlq.orders.queue", 2)
//	if err != nil {
//	    log.Fatal(err)
//	}
func (b *EventBus) StartConsumeDLQ(queue string, workers int) error {
	if b.dlqRouter == nil {
		return errors.ErrNoHandlersRegistered
	}

	// Create a separate consumer for DLQ with the DLQ router
	dlqConsumer := broker.NewConsumer(b.client, b.publisher, b.dlqRouter)

	return dlqConsumer.Consume(queue, workers)
}

// RequeueFromDLQ re-enqueues a message from DLQ back to its original queue for reprocessing.
//
// This allows you to retry failed messages after fixing the underlying issue
// (e.g., after deploying a bug fix, restoring a downstream service, etc.).
//
// Parameters:
//   - ctx: context for the operation
//   - dlqMsg: the DLQ message to re-enqueue
//   - resetRetryCount: if true, resets the retry count to 0 (gives full retry attempts)
//
// The message will be published back to the original exchange with the original
// routing key, so it will be routed to the original queue.
//
// Example:
//
//	eventBus.RegisterDLQHandler("order.created", func(msgCtx *router.MessageContext) error {
//	    dlqMsg := router.NewDLQMessage(msgCtx)
//
//	    // Check if we should retry
//	    if dlqMsg.ShouldRetry(5) {
//	        // Re-enqueue with reset retry count
//	        err := eventBus.RequeueFromDLQ(context.Background(), dlqMsg, true)
//	        if err != nil {
//	            return err
//	        }
//	        // Ack the DLQ message after successful re-enqueue
//	        return msgCtx.Ack()
//	    }
//
//	    return nil
//	})
func (b *EventBus) RequeueFromDLQ(ctx context.Context, dlqMsg *router.DLQMessage, resetRetryCount bool) error {
	if dlqMsg.OriginalExchange == "" || dlqMsg.OriginalRoutingKey == "" {
		return errors.NewConfigFieldError("DLQMessage", "missing original exchange or routing key")
	}

	// Prepare the message body
	body := dlqMsg.Body()

	// Prepare headers
	headers := make(map[string]interface{})
	for k, v := range dlqMsg.Delivery.Headers {
		headers[k] = v
	}

	// Reset or preserve retry count
	if resetRetryCount {
		headers["x-retry-count"] = int32(0)
	}

	// Remove x-death header to avoid confusion
	delete(headers, "x-death")

	// Publish back to original destination
	return b.publisher.Publish(ctx, dlqMsg.OriginalExchange, dlqMsg.OriginalRoutingKey, amqp.Publishing{
		ContentType:  dlqMsg.Delivery.ContentType,
		Body:         body,
		DeliveryMode: dlqMsg.Delivery.DeliveryMode,
		Priority:     dlqMsg.Delivery.Priority,
		Timestamp:    time.Now(),
		Headers:      headers,
	})
}

// RequeueAllFromDLQ re-enqueues all messages from a DLQ back to their original queues.
//
// This is useful for bulk recovery after fixing an issue that caused many messages
// to fail. Messages are consumed from the DLQ, published back to their original
// destinations, and then acknowledged.
//
// Parameters:
//   - ctx: context for the operation
//   - dlqName: name of the DLQ to drain (e.g., "dlq.orders.queue")
//   - resetRetryCount: if true, resets retry count for all messages
//   - maxMessages: maximum number of messages to requeue (0 = unlimited)
//
// Returns the number of messages successfully requeued and any error encountered.
//
// Example:
//
//	// Requeue up to 100 messages with reset retry count
//	count, err := eventBus.RequeueAllFromDLQ(
//	    context.Background(),
//	    "dlq.orders.queue",
//	    true,  // reset retry count
//	    100,   // max 100 messages
//	)
//	if err != nil {
//	    log.Printf("Error requeuing: %v", err)
//	}
//	log.Printf("Successfully requeued %d messages", count)
func (b *EventBus) RequeueAllFromDLQ(ctx context.Context, dlqName string, resetRetryCount bool, maxMessages int) (int, error) {
	// Get channel via client's GetChannel method
	channel, err := b.client.GetChannel()
	if err != nil {
		return 0, err
	}

	requeuedCount := 0

	// Consume messages from DLQ
	deliveries, err := channel.Consume(
		dlqName,
		"",    // consumer tag
		false, // auto-ack (manual ack to ensure we don't lose messages)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return 0, errors.NewConsumeError(dlqName, err)
	}

	// Process messages until done or max reached
	for delivery := range deliveries {
		// Check if we've reached the limit
		if maxMessages > 0 && requeuedCount >= maxMessages {
			// Nack remaining message to keep it in DLQ
			_ = delivery.Nack(false, true)
			break
		}

		// Check context cancellation
		if ctx.Err() != nil {
			_ = delivery.Nack(false, true)
			return requeuedCount, ctx.Err()
		}

		// Create DLQ message from delivery
		msgCtx := &router.MessageContext{Delivery: delivery}
		dlqMsg := router.NewDLQMessage(msgCtx)

		// Try to requeue
		err := b.RequeueFromDLQ(ctx, dlqMsg, resetRetryCount)
		if err != nil {
			// Log error - can't access logger directly, continue silently
			// Nack and requeue in DLQ
			_ = delivery.Nack(false, true)
			continue
		}

		// Successfully requeued, ack the DLQ message
		_ = delivery.Ack(false) // Ignore error - can't access logger directly

		requeuedCount++

		// If this was the last message in DLQ, the channel will close
		// We detect this by trying to peek at the next iteration
	}

	return requeuedCount, nil
}
