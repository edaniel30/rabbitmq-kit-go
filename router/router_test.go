package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test helpers ---

// mockHandler implements HandlerService for testing
type mockHandler struct {
	executed bool
}

func (h *mockHandler) Execute(ctx *MessageContext) error {
	h.executed = true
	return nil
}

// --- NewRouter ---

func TestNewRouter(t *testing.T) {
	t.Run("creates router with empty handlers map", func(t *testing.T) {
		r := NewRouter()

		require.NotNil(t, r)
		assert.NotNil(t, r.handlers)
		assert.Empty(t, r.handlers)
	})
}

// --- Handle ---

func TestHandle(t *testing.T) {
	t.Run("registers handler for event type", func(t *testing.T) {
		r := NewRouter()
		handler := &mockHandler{}

		r.Handle("user.created", handler)

		assert.NotNil(t, r.handlers["user.created"])
	})

	t.Run("overwrites existing handler for same event type", func(t *testing.T) {
		r := NewRouter()
		handler1 := &mockHandler{}
		handler2 := &mockHandler{}

		r.Handle("event.type", handler1)
		r.Handle("event.type", handler2)

		assert.Len(t, r.handlers, 1)
		assert.Equal(t, handler2, r.GetHandler("event.type"))
	})

	t.Run("registers multiple handlers for different event types", func(t *testing.T) {
		r := NewRouter()

		r.Handle("user.created", &mockHandler{})
		r.Handle("user.updated", &mockHandler{})
		r.Handle("order.created", &mockHandler{})

		assert.Len(t, r.handlers, 3)
	})
}

// --- GetHandler ---

func TestGetHandler(t *testing.T) {
	t.Run("returns registered handler", func(t *testing.T) {
		r := NewRouter()
		handler := &mockHandler{}

		r.Handle("user.created", handler)

		retrieved := r.GetHandler("user.created")
		require.NotNil(t, retrieved)
	})

	t.Run("returns nil for unregistered event type", func(t *testing.T) {
		r := NewRouter()

		assert.Nil(t, r.GetHandler("non.existent"))
	})
}
