package rabbitmq

import (
	"time"

	"github.com/google/uuid"
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
}

// BaseEvent provides a basic implementation of the Event interface.
//
// It includes common fields that most events need:
//   - ID: unique identifier for the event
//   - OccurredAt: timestamp when the event occurred
//   - Type: the event type (routing key)
//   - Exchange: the exchange to publish to
//
// Domain events should embed BaseEvent and add their specific fields.
//
// Example:
//
//	type UserCreatedEvent struct {
//	    *rabbitmq.BaseEvent
//	    UserID   string
//	    Username string
//	    Email    string
//	}
//
//	func NewUserCreatedEvent(userID, username, email string) *UserCreatedEvent {
//	    return &UserCreatedEvent{
//	        BaseEvent: rabbitmq.NewBaseEvent("users.exchange", "user.created"),
//	        UserID:    userID,
//	        Username:  username,
//	        Email:     email,
//	    }
//	}
//
//	func (e *UserCreatedEvent) ToMap() map[string]any {
//	    m := e.BaseEvent.ToMap()
//	    m["user_id"] = e.UserID
//	    m["username"] = e.Username
//	    m["email"] = e.Email
//	    return m
//	}
type BaseEvent struct {
	id         string
	occurredAt time.Time
	eventType  string
	exchange   string
}

// NewBaseEvent creates a new base event with the given exchange and type.
//
// The ID is generated automatically (you can set it with SetID if needed).
// The OccurredAt timestamp is set to the current time.
//
// Example:
//
//	event := rabbitmq.NewBaseEvent("orders.exchange", "order.created")
func NewBaseEvent(exchange, eventType string) *BaseEvent {
	return &BaseEvent{
		id:         uuid.New().String(),
		occurredAt: time.Now(),
		eventType:  eventType,
		exchange:   exchange,
	}
}

// NewBaseEventWithID creates a new base event with a specific ID.
//
// Use this when the event ID should match a domain entity ID.
//
// Example:
//
//	event := rabbitmq.NewBaseEventWithID(order.ID, "orders.exchange", "order.created")
func NewBaseEventWithID(id, exchange, eventType string) *BaseEvent {
	return &BaseEvent{
		id:         id,
		occurredAt: time.Now(),
		eventType:  eventType,
		exchange:   exchange,
	}
}

// Type returns the event type (routing key).
func (e *BaseEvent) Type() string {
	return e.eventType
}

// Exchange returns the exchange name.
func (e *BaseEvent) Exchange() string {
	return e.exchange
}

// ToMap converts the base event fields to a map.
//
// Domain events that embed BaseEvent should call this method and add
// their specific fields to the returned map.
func (e *BaseEvent) ToMap() map[string]any {
	return map[string]any{
		"id":          e.id,
		"occurred_at": e.occurredAt,
		"type":        e.eventType,
	}
}

// ID returns the event ID.
func (e *BaseEvent) ID() string {
	return e.id
}

// OccurredAt returns the event timestamp.
func (e *BaseEvent) OccurredAt() time.Time {
	return e.occurredAt
}

// SetID sets the event ID (useful for correlating with domain entities).
func (e *BaseEvent) SetID(id string) {
	e.id = id
}
