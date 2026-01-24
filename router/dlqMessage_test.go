package router

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDLQMessage(t *testing.T) {
	t.Run("creates DLQ message with x-death header", func(t *testing.T) {
		now := time.Now()
		xDeath := []interface{}{
			amqp.Table{
				"count":        int64(3),
				"reason":       "rejected",
				"queue":        "original.queue",
				"time":         now,
				"exchange":     "orders.exchange",
				"routing-keys": []interface{}{"order.created"},
			},
		}

		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-death":       xDeath,
					"x-retry-count": int32(2),
				},
				Body: []byte("test body"),
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 3, dlq.DeathCount)
		assert.Equal(t, "rejected", dlq.DeathReason)
		assert.Equal(t, "original.queue", dlq.OriginalQueue)
		assert.Equal(t, "orders.exchange", dlq.OriginalExchange)
		assert.Equal(t, "order.created", dlq.OriginalRoutingKey)
		assert.Equal(t, 2, dlq.RetryCount)
		assert.NotNil(t, dlq.Delivery)
	})

	t.Run("handles missing x-death header", func(t *testing.T) {
		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{},
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
		assert.Empty(t, dlq.DeathReason)
		assert.Empty(t, dlq.OriginalQueue)
	})

	t.Run("handles invalid x-death format", func(t *testing.T) {
		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-death": "invalid",
				},
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
	})

	t.Run("handles empty x-death array", func(t *testing.T) {
		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-death": []interface{}{},
				},
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
	})

	t.Run("handles int32 death count", func(t *testing.T) {
		xDeath := []interface{}{
			amqp.Table{
				"count":  int32(5),
				"reason": "expired",
				"queue":  "test.queue",
			},
		}

		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-death": xDeath,
				},
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 5, dlq.DeathCount)
		assert.Equal(t, "expired", dlq.DeathReason)
	})
}

func TestDLQMessage_ShouldRetry(t *testing.T) {
	t.Run("should retry when below max", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 2}
		assert.True(t, dlq.ShouldRetry(5))
		assert.True(t, dlq.ShouldRetry(3))
	})

	t.Run("should not retry when at or above max", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 5}
		assert.False(t, dlq.ShouldRetry(5))
		assert.False(t, dlq.ShouldRetry(3))
	})

	t.Run("handles zero max retries", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 0}
		assert.False(t, dlq.ShouldRetry(0))
	})
}

func TestDLQMessage_GetDeathInfo(t *testing.T) {
	t.Run("formats death info correctly", func(t *testing.T) {
		dlq := &DLQMessage{
			DeathCount:          3,
			DeathReason:         "rejected",
			OriginalQueue:       "orders.queue",
			OriginalExchange:    "orders.exchange",
			OriginalRoutingKey:  "order.created",
			RetryCount:          2,
			FirstDeathTimestamp: time.Now().Unix(),
		}

		info := dlq.GetDeathInfo()

		assert.Contains(t, info, "death_count=3")
		assert.Contains(t, info, "reason=rejected")
		assert.Contains(t, info, "queue=orders.queue")
		assert.Contains(t, info, "exchange=orders.exchange")
		assert.Contains(t, info, "routing_key=order.created")
	})

	t.Run("handles zero values", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{},
			},
		}

		info := dlq.GetDeathInfo()

		assert.Contains(t, info, "death_count=0")
		assert.Contains(t, info, "retry_count=0")
	})
}

func TestDLQMessage_ResetRetryCount(t *testing.T) {
	t.Run("returns copy with retry count reset", func(t *testing.T) {
		original := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{
					Headers: amqp.Table{
						"x-retry-count": int32(5),
					},
				},
			},
			RetryCount: 5,
		}

		reset := original.ResetRetryCount()

		assert.Equal(t, 0, reset.RetryCount)
		assert.Equal(t, 5, original.RetryCount) // Original unchanged
		assert.NotEqual(t, original, reset)     // Different instances
	})
}

func TestDLQMessage_GetOriginalDestination(t *testing.T) {
	t.Run("returns original queue", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{},
			},
			OriginalQueue: "orders.queue",
		}

		queue := dlq.GetOriginalDestination()
		assert.Equal(t, "orders.queue", queue)
	})

	t.Run("returns empty string when not set", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{
					Headers: amqp.Table{},
				},
			},
		}

		queue := dlq.GetOriginalDestination()
		assert.Empty(t, queue)
	})

	t.Run("falls back to x-first-death-queue header", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{
					Headers: amqp.Table{
						"x-first-death-queue": "fallback.queue",
					},
				},
			},
			OriginalQueue: "",
		}

		queue := dlq.GetOriginalDestination()
		assert.Equal(t, "fallback.queue", queue)
	})
}

func TestDLQMessage_Body(t *testing.T) {
	t.Run("returns message body from embedded MessageContext", func(t *testing.T) {
		body := []byte("test message body")
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{Body: body},
			},
		}

		result := dlq.Body()
		assert.Equal(t, body, result)
	})
}
