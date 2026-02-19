package router

import (
	"encoding/json"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test helpers ---

// mockAcknowledger implements amqp.Acknowledger for testing
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

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	return nil
}

// --- BindJSON ---

func TestMessageContext_BindJSON(t *testing.T) {
	type User struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	t.Run("successfully binds JSON", func(t *testing.T) {
		body, _ := json.Marshal(User{ID: "123", Name: "John"})
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: body}}

		var result User
		err := ctx.BindJSON(&result)

		require.NoError(t, err)
		assert.Equal(t, "123", result.ID)
		assert.Equal(t, "John", result.Name)
	})

	t.Run("returns error for invalid JSON", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: []byte("invalid json")}}

		var result User
		assert.Error(t, ctx.BindJSON(&result))
	})
}

// --- GetType ---

func TestMessageContext_GetType(t *testing.T) {
	t.Run("returns type from JSON body", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"type": "user.created", "data": "test"})
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: body}}

		assert.Equal(t, "user.created", ctx.GetType())
	})

	t.Run("returns empty string for invalid JSON", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: []byte("invalid")}}

		assert.Empty(t, ctx.GetType())
	})

	t.Run("returns empty string when type field missing", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"data": "test"})
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: body}}

		assert.Empty(t, ctx.GetType())
	})
}

// --- Body ---

func TestMessageContext_Body(t *testing.T) {
	t.Run("returns raw message body", func(t *testing.T) {
		body := []byte("test message")
		ctx := &MessageContext{Delivery: amqp.Delivery{Body: body}}

		assert.Equal(t, body, ctx.Body())
	})
}

// --- GetHeader ---

func TestMessageContext_GetHeader(t *testing.T) {
	t.Run("returns existing int32 header", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{"x-retry-count": int32(3)}},
		}
		assert.Equal(t, int32(3), ctx.GetHeader("x-retry-count"))
	})

	t.Run("returns existing string header", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{"custom-header": "value"}},
		}
		assert.Equal(t, "value", ctx.GetHeader("custom-header"))
	})

	t.Run("returns nil for non-existent header", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Headers: amqp.Table{}}}

		assert.Nil(t, ctx.GetHeader("non-existent"))
	})

	t.Run("returns nil when headers are nil", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Headers: nil}}

		assert.Nil(t, ctx.GetHeader("any-key"))
	})
}

// --- GetRetryCount ---

func TestMessageContext_GetRetryCount(t *testing.T) {
	t.Run("returns retry count from header", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{"x-retry-count": int32(5)}},
		}
		assert.Equal(t, 5, ctx.GetRetryCount())
	})

	t.Run("returns 0 when header missing", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Headers: amqp.Table{}}}

		assert.Equal(t, 0, ctx.GetRetryCount())
	})

	t.Run("returns 0 when headers are nil", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Headers: nil}}

		assert.Equal(t, 0, ctx.GetRetryCount())
	})

	t.Run("handles different numeric types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected int
		}{
			{"int32", int32(3), 3},
			{"int64", int64(7), 7},
			{"int", int(2), 2},
			{"float64", float64(5.0), 5},
			{"invalid type", "string", 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := &MessageContext{
					Delivery: amqp.Delivery{
						Headers: amqp.Table{"x-retry-count": tt.value},
					},
				}
				assert.Equal(t, tt.expected, ctx.GetRetryCount())
			})
		}
	})
}

// --- Ack ---

func TestMessageContext_Ack(t *testing.T) {
	t.Run("acknowledges delivery", func(t *testing.T) {
		mock := &mockAcknowledger{}
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Acknowledger: mock, DeliveryTag: 1},
		}

		require.NoError(t, ctx.Ack())
		assert.True(t, mock.acked)
	})
}

// --- Nack ---

func TestMessageContext_Nack(t *testing.T) {
	t.Run("nacks with requeue true", func(t *testing.T) {
		mock := &mockAcknowledger{}
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Acknowledger: mock, DeliveryTag: 1},
		}

		require.NoError(t, ctx.Nack(true))
		assert.True(t, mock.nacked)
		assert.True(t, mock.requeued)
	})

	t.Run("nacks with requeue false", func(t *testing.T) {
		mock := &mockAcknowledger{}
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Acknowledger: mock, DeliveryTag: 1},
		}

		require.NoError(t, ctx.Nack(false))
		assert.True(t, mock.nacked)
		assert.False(t, mock.requeued)
	})
}

// --- GetTraceID ---

func TestMessageContext_GetTraceID(t *testing.T) {
	t.Run("returns trace_id from headers", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{"trace_id": "abc-123"}},
		}
		assert.Equal(t, "abc-123", ctx.GetTraceID())
	})

	t.Run("returns empty string when trace_id missing", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{Headers: amqp.Table{}}}

		assert.Equal(t, "", ctx.GetTraceID())
	})

	t.Run("returns empty string when headers nil", func(t *testing.T) {
		ctx := &MessageContext{Delivery: amqp.Delivery{}}

		assert.Equal(t, "", ctx.GetTraceID())
	})
}
