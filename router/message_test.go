package router

import (
	"encoding/json"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageContext_BindJSON(t *testing.T) {
	type User struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	t.Run("successfully binds JSON", func(t *testing.T) {
		user := User{ID: "123", Name: "John"}
		body, _ := json.Marshal(user)

		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: body},
		}

		var result User
		err := ctx.BindJSON(&result)

		require.NoError(t, err)
		assert.Equal(t, "123", result.ID)
		assert.Equal(t, "John", result.Name)
	})

	t.Run("fails with invalid JSON", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: []byte("invalid json")},
		}

		var result User
		err := ctx.BindJSON(&result)

		require.Error(t, err)
	})
}

func TestMessageContext_GetType(t *testing.T) {
	t.Run("gets type from JSON body", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{
			"type": "user.created",
			"data": "test",
		})

		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: body},
		}

		eventType := ctx.GetType()
		assert.Equal(t, "user.created", eventType)
	})

	t.Run("returns empty string for invalid JSON", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: []byte("invalid")},
		}

		eventType := ctx.GetType()
		assert.Empty(t, eventType)
	})

	t.Run("returns empty string when type field missing", func(t *testing.T) {
		body, _ := json.Marshal(map[string]any{"data": "test"})

		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: body},
		}

		eventType := ctx.GetType()
		assert.Empty(t, eventType)
	})
}

func TestMessageContext_Body(t *testing.T) {
	t.Run("returns message body", func(t *testing.T) {
		body := []byte("test message")
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Body: body},
		}

		result := ctx.Body()
		assert.Equal(t, body, result)
	})
}

func TestMessageContext_GetHeader(t *testing.T) {
	t.Run("gets existing header", func(t *testing.T) {
		headers := amqp.Table{
			"x-retry-count": int32(3),
			"custom-header": "value",
		}

		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: headers},
		}

		value := ctx.GetHeader("x-retry-count")
		assert.Equal(t, int32(3), value)

		value = ctx.GetHeader("custom-header")
		assert.Equal(t, "value", value)
	})

	t.Run("returns nil for non-existent header", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{}},
		}

		value := ctx.GetHeader("non-existent")
		assert.Nil(t, value)
	})

	t.Run("returns nil when headers are nil", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: nil},
		}

		value := ctx.GetHeader("any-key")
		assert.Nil(t, value)
	})
}

func TestMessageContext_GetRetryCount(t *testing.T) {
	t.Run("gets retry count from header", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{"x-retry-count": int32(5)},
			},
		}

		count := ctx.GetRetryCount()
		assert.Equal(t, 5, count)
	})

	t.Run("returns 0 when header missing", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{}},
		}

		count := ctx.GetRetryCount()
		assert.Equal(t, 0, count)
	})

	t.Run("returns 0 when headers are nil", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{Headers: nil},
		}

		count := ctx.GetRetryCount()
		assert.Equal(t, 0, count)
	})

	t.Run("handles different integer types", func(t *testing.T) {
		tests := []struct {
			name     string
			value    any
			expected int
		}{
			{"int32", int32(3), 3},
			{"int64", int64(7), 7},
			{"int", int(2), 2},
			{"float64", float64(5.0), 5},
			{"invalid", "string", 0},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := &MessageContext{
					Delivery: amqp.Delivery{
						Headers: amqp.Table{"x-retry-count": tt.value},
					},
				}

				count := ctx.GetRetryCount()
				assert.Equal(t, tt.expected, count)
			})
		}
	})
}

func TestMessageContext_Ack(t *testing.T) {
	t.Run("calls Ack on delivery", func(t *testing.T) {
		ctx := &MessageContext{
			Delivery: amqp.Delivery{
				Acknowledger: &mockAcknowledger{},
				DeliveryTag:  1,
			},
		}

		err := ctx.Ack()
		assert.NoError(t, err)
		assert.True(t, ctx.Delivery.Acknowledger.(*mockAcknowledger).acked)
	})
}

func TestMessageContext_Nack(t *testing.T) {
	t.Run("calls Nack with requeue true", func(t *testing.T) {
		mock := &mockAcknowledger{}
		ctx := &MessageContext{
			Delivery: amqp.Delivery{
				Acknowledger: mock,
				DeliveryTag:  1,
			},
		}

		err := ctx.Nack(true)
		assert.NoError(t, err)
		assert.True(t, mock.nacked)
		assert.True(t, mock.requeued)
	})

	t.Run("calls Nack with requeue false", func(t *testing.T) {
		mock := &mockAcknowledger{}
		ctx := &MessageContext{
			Delivery: amqp.Delivery{
				Acknowledger: mock,
				DeliveryTag:  1,
			},
		}

		err := ctx.Nack(false)
		assert.NoError(t, err)
		assert.True(t, mock.nacked)
		assert.False(t, mock.requeued)
	})
}

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
