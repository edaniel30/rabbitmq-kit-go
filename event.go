package rabbitmq

import (
	"context"
	"encoding/json"
	"maps"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/internal/tracing"
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
// The trace context of ctx (the publish span) is injected into the headers (W3C traceparent),
// so the consumer's span joins the same trace. If the event sets no trace_id header (or an
// empty one), it is the OpenTelemetry trace ID of ctx, or a new UUID without telemetry.
//
// The return value is the publishing and an error if the event cannot be marshaled.
func createDefaultPublishing(ctx context.Context, event Event) (amqp.Publishing, error) {
	headersToSend := amqp.Table{}
	if headers := event.Headers(); headers != nil {
		maps.Copy(headersToSend, headers)
	}

	if traceID, _ := headersToSend["trace_id"].(string); traceID == "" {
		if traceID = tracing.TraceID(ctx); traceID == "" {
			traceID = uuid.New().String()
		}
		headersToSend["trace_id"] = traceID
	}

	tracing.Inject(ctx, headersToSend)

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
