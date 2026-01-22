package rabbitmq

import (
	"context"
	"encoding/json"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/broker"
	"github.com/edaniel30/rabbitmq-kit-go/router"
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

// Publish publishes a single event to RabbitMQ.
//
// The event's Exchange() and Type() methods determine where the message
// is published. The ToMap() method is used to serialize the event to JSON.
//
// Example:
//
//	event := NewUserCreatedEvent(user.ID, user.Name, user.Email)
//	err := eventBus.Publish(ctx, event)
func (b *EventBus) Publish(ctx context.Context, event Event) error {
	// Serialize event to JSON
	body, err := json.Marshal(event.ToMap())
	if err != nil {
		return err
	}

	// Publish to RabbitMQ
	return b.publisher.Publish(ctx, event.Exchange(), event.Type(), body)
}

// PublishBatch publishes multiple events in sequence.
//
// This is useful for publishing events that were accumulated in a domain
// aggregate during a transaction. All events are published with the same
// context.
//
// If any event fails to publish, the method stops and returns the error.
// Previously published events are NOT rolled back (RabbitMQ doesn't support
// transactions across multiple publishes without using AMQP transactions,
// which are not recommended for performance reasons).
//
// Example:
//
//	// In your domain aggregate
//	order.AddProduct(productID, quantity)
//	order.RemoveProduct(otherProductID)
//
//	// In your application layer (after successful DB commit)
//	events := order.PullEvents()  // Returns []Event
//	err := eventBus.PublishBatch(ctx, events)
func (b *EventBus) PublishBatch(ctx context.Context, events []Event) error {
	for _, event := range events {
		if err := b.Publish(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// PublishBatchAsync publishes multiple events concurrently.
//
// This is faster than PublishBatch for large batches, but events may be
// published out of order. Use this only when event ordering doesn't matter.
//
// Returns the first error encountered, but other goroutines may continue
// publishing. If you need all-or-nothing semantics, use PublishBatch.
//
// Example:
//
//	events := []rabbitmq.Event{event1, event2, event3}
//	err := eventBus.PublishBatchAsync(ctx, events)
func (b *EventBus) PublishBatchAsync(ctx context.Context, events []Event) error {
	errChan := make(chan error, len(events))

	for _, event := range events {
		go func(e Event) {
			errChan <- b.Publish(ctx, e)
		}(event)
	}

	// Wait for all publishes to complete
	for range events {
		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
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

// Close closes the underlying RabbitMQ client if owned by this EventBus.
//
// If the EventBus was created with NewEventBusFromClient(), this method
// does nothing (you must close the client yourself).
func (b *EventBus) Close() error {
	return b.client.Close()
}

// IsConnected returns true if the underlying client is connected.
func (b *EventBus) IsConnected() bool {
	return b.client.IsConnected()
}
