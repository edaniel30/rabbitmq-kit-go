package router

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockHandler implements HandlerService for testing
type mockHandler struct {
	executed bool
}

func (h *mockHandler) Execute(ctx *MessageContext) error {
	h.executed = true
	return nil
}

func TestRouter(t *testing.T) {
	t.Run("NewRouter creates empty router", func(t *testing.T) {
		r := NewRouter()

		require.NotNil(t, r)
		assert.NotNil(t, r.handlers)
		assert.Empty(t, r.handlers)
	})

	t.Run("Handle registers handler and GetHandler retrieves it", func(t *testing.T) {
		r := NewRouter()
		handler := &mockHandler{}

		// Register handler
		r.Handle("user.created", handler)

		// Retrieve handler
		retrieved := r.GetHandler("user.created")
		require.NotNil(t, retrieved)

		// Verify handler exists
		assert.NotNil(t, r.handlers["user.created"])
	})

	t.Run("GetHandler returns nil for unregistered event", func(t *testing.T) {
		r := NewRouter()

		handler := r.GetHandler("non.existent")
		assert.Nil(t, handler)
	})

	t.Run("Handle overwrites existing handler", func(t *testing.T) {
		r := NewRouter()
		handler1 := &mockHandler{}
		handler2 := &mockHandler{}

		r.Handle("event.type", handler1)
		r.Handle("event.type", handler2)

		// Should only have one handler
		assert.Len(t, r.handlers, 1)
		assert.Equal(t, handler2, r.GetHandler("event.type"))
	})

	t.Run("multiple handlers for different events", func(t *testing.T) {
		r := NewRouter()

		r.Handle("user.created", &mockHandler{})
		r.Handle("user.updated", &mockHandler{})
		r.Handle("order.created", &mockHandler{})

		assert.Len(t, r.handlers, 3)
		assert.NotNil(t, r.GetHandler("user.created"))
		assert.NotNil(t, r.GetHandler("user.updated"))
		assert.NotNil(t, r.GetHandler("order.created"))
		assert.Nil(t, r.GetHandler("non.existent"))
	})
}
