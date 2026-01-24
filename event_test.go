package rabbitmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBaseEvent(t *testing.T) {
	t.Run("creates event with generated ID", func(t *testing.T) {
		event := NewBaseEvent("test.exchange", "test.event")

		assert.NotEmpty(t, event.ID())
		assert.Equal(t, "test.exchange", event.Exchange())
		assert.Equal(t, "test.event", event.Type())
		assert.WithinDuration(t, time.Now(), event.OccurredAt(), 1*time.Second)
	})

	t.Run("each event has unique ID", func(t *testing.T) {
		event1 := NewBaseEvent("ex", "type")
		event2 := NewBaseEvent("ex", "type")

		assert.NotEqual(t, event1.ID(), event2.ID())
	})
}

func TestNewBaseEventWithID(t *testing.T) {
	t.Run("creates event with specific ID", func(t *testing.T) {
		customID := "order-12345"
		event := NewBaseEventWithID(customID, "orders.exchange", "order.created")

		assert.Equal(t, customID, event.ID())
		assert.Equal(t, "orders.exchange", event.Exchange())
		assert.Equal(t, "order.created", event.Type())
		assert.WithinDuration(t, time.Now(), event.OccurredAt(), 1*time.Second)
	})
}

func TestBaseEvent_Type(t *testing.T) {
	event := NewBaseEvent("ex", "user.created")
	assert.Equal(t, "user.created", event.Type())
}

func TestBaseEvent_Exchange(t *testing.T) {
	event := NewBaseEvent("events.exchange", "test")
	assert.Equal(t, "events.exchange", event.Exchange())
}

func TestBaseEvent_ID(t *testing.T) {
	event := NewBaseEventWithID("custom-id-123", "ex", "type")
	assert.Equal(t, "custom-id-123", event.ID())
}

func TestBaseEvent_OccurredAt(t *testing.T) {
	before := time.Now()
	event := NewBaseEvent("ex", "type")
	after := time.Now()

	occurredAt := event.OccurredAt()
	assert.True(t, occurredAt.After(before) || occurredAt.Equal(before))
	assert.True(t, occurredAt.Before(after) || occurredAt.Equal(after))
}

func TestBaseEvent_SetID(t *testing.T) {
	t.Run("can change event ID", func(t *testing.T) {
		event := NewBaseEvent("ex", "type")
		originalID := event.ID()

		newID := "new-custom-id"
		event.SetID(newID)

		assert.NotEqual(t, originalID, event.ID())
		assert.Equal(t, newID, event.ID())
	})
}

func TestBaseEvent_ToMap(t *testing.T) {
	t.Run("converts base event to map", func(t *testing.T) {
		event := NewBaseEventWithID("test-id", "test.exchange", "test.event")

		m := event.ToMap()

		assert.Equal(t, "test-id", m["id"])
		assert.Equal(t, "test.event", m["type"])
		assert.IsType(t, time.Time{}, m["occurred_at"])
		assert.NotNil(t, m["occurred_at"])
	})

	t.Run("map contains all required fields", func(t *testing.T) {
		event := NewBaseEvent("ex", "type")
		m := event.ToMap()

		assert.Contains(t, m, "id")
		assert.Contains(t, m, "occurred_at")
		assert.Contains(t, m, "type")
		assert.Len(t, m, 3)
	})
}

func TestBaseEvent_EventInterface(t *testing.T) {
	t.Run("BaseEvent implements Event interface", func(t *testing.T) {
		var _ Event = (*BaseEvent)(nil)
	})

	t.Run("BaseEvent can be used as Event", func(t *testing.T) {
		var event Event = NewBaseEvent("test.exchange", "test.event")

		assert.Equal(t, "test.exchange", event.Exchange())
		assert.Equal(t, "test.event", event.Type())
		assert.NotNil(t, event.ToMap())
	})
}

// TestCustomEvent tests embedding BaseEvent in custom events
func TestCustomEvent(t *testing.T) {
	// Define a custom event type
	type UserCreatedEvent struct {
		*BaseEvent
		UserID   string
		Username string
		Email    string
	}

	t.Run("custom event embeds BaseEvent", func(t *testing.T) {
		customEvent := &UserCreatedEvent{
			BaseEvent: NewBaseEvent("users.exchange", "user.created"),
			UserID:    "user-123",
			Username:  "johndoe",
			Email:     "john@example.com",
		}

		assert.Equal(t, "users.exchange", customEvent.Exchange())
		assert.Equal(t, "user.created", customEvent.Type())
		assert.Equal(t, "user-123", customEvent.UserID)
		assert.Equal(t, "johndoe", customEvent.Username)
		assert.Equal(t, "john@example.com", customEvent.Email)
		assert.NotEmpty(t, customEvent.ID())
	})

	t.Run("custom event can extend ToMap", func(t *testing.T) {
		customEvent := &UserCreatedEvent{
			BaseEvent: NewBaseEvent("users.exchange", "user.created"),
			UserID:    "user-456",
			Username:  "janedoe",
			Email:     "jane@example.com",
		}

		// Simulate extending ToMap
		m := customEvent.ToMap()
		m["user_id"] = customEvent.UserID
		m["username"] = customEvent.Username
		m["email"] = customEvent.Email

		assert.Equal(t, "user-456", m["user_id"])
		assert.Equal(t, "janedoe", m["username"])
		assert.Equal(t, "jane@example.com", m["email"])
		assert.Contains(t, m, "id")
		assert.Contains(t, m, "occurred_at")
		assert.Contains(t, m, "type")
	})

	t.Run("custom event implements Event interface", func(t *testing.T) {
		var event Event = &UserCreatedEvent{
			BaseEvent: NewBaseEvent("users.exchange", "user.created"),
			UserID:    "user-789",
			Username:  "testuser",
			Email:     "test@example.com",
		}

		assert.NotNil(t, event)
		assert.Equal(t, "users.exchange", event.Exchange())
		assert.Equal(t, "user.created", event.Type())
	})
}
