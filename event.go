package rabbitmq

import (
	"encoding/json"
	"maps"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Event represents a domain event that can be published to RabbitMQ.
//
// Events are used in Domain-Driven Design to represent something that
// happened in the domain. They are typically published after a successful
// domain operation and consumed by other services or bounded contexts.
type Event interface {
	// Type returns the event type (e.g., "user.created", "order.completed")
	// This is used as the routing key when publishing.
	Type() string

	// Exchange returns the exchange name where this event should be published.
	Exchange() string

	// ToMap converts the event to a map for JSON serialization.
	// This map will be marshaled and sent as the message body.
	ToMap() map[string]any

	// Headers returns the headers for the event.
	Headers() map[string]any
}

// createDefaultPublishing creates a default publishing for the event.
//
// The body is the event to JSON.
//
// The return value is the publishing and an error if the event cannot be marshaled.
func createDefaultPublishing(event Event) (amqp.Publishing, error) {
	headersToSend := make(map[string]any)
	// Sent new traceid header if not present
	if headers := event.Headers(); headers != nil {
		maps.Copy(headersToSend, headers)
	}

	if headersToSend["trace_id"] == nil {
		headersToSend["trace_id"] = uuid.New().String()
	}

	body, err := json.Marshal(event.ToMap())
	if err != nil {
		return amqp.Publishing{}, err
	}

	msg := amqp.Publishing{
		Headers:       headersToSend,
		ContentType:   "application/json",
		DeliveryMode:  amqp.Persistent,
		CorrelationId: uuid.New().String(),
		MessageId:     uuid.New().String(),
		Timestamp:     time.Now(),
		Type:          event.Type(),
		Body:          body,
	}

	return msg, nil
}
