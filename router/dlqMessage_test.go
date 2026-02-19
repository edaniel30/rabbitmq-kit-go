package router

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- NewDLQMessage ---

func TestNewDLQMessage(t *testing.T) {
	t.Run("extracts all fields from x-death header", func(t *testing.T) {
		now := time.Now()
		msgCtx := &MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-retry-count": int32(2),
					"x-death": []any{
						amqp.Table{
							"count":        int64(3),
							"reason":       "rejected",
							"queue":        "original.queue",
							"time":         now,
							"exchange":     "orders.exchange",
							"routing-keys": []any{"order.created"},
						},
					},
				},
				Body: []byte("test body"),
			},
		}

		dlq := NewDLQMessage(msgCtx)

		require.NotNil(t, dlq)
		assert.Equal(t, 2, dlq.RetryCount)
		assert.Equal(t, 3, dlq.DeathCount)
		assert.Equal(t, "rejected", dlq.DeathReason)
		assert.Equal(t, "original.queue", dlq.OriginalQueue)
		assert.Equal(t, "orders.exchange", dlq.OriginalExchange)
		assert.Equal(t, "order.created", dlq.OriginalRoutingKey)
		assert.Equal(t, now.Unix(), dlq.FirstDeathTimestamp)
	})

	t.Run("handles missing x-death header", func(t *testing.T) {
		dlq := NewDLQMessage(&MessageContext{
			Delivery: amqp.Delivery{Headers: amqp.Table{}},
		})

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
		assert.Empty(t, dlq.DeathReason)
		assert.Empty(t, dlq.OriginalQueue)
	})

	t.Run("handles invalid x-death format", func(t *testing.T) {
		dlq := NewDLQMessage(&MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{"x-death": "invalid"},
			},
		})

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
	})

	t.Run("handles empty x-death array", func(t *testing.T) {
		dlq := NewDLQMessage(&MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{"x-death": []any{}},
			},
		})

		require.NotNil(t, dlq)
		assert.Equal(t, 0, dlq.DeathCount)
	})

	t.Run("handles int32 death count", func(t *testing.T) {
		dlq := NewDLQMessage(&MessageContext{
			Delivery: amqp.Delivery{
				Headers: amqp.Table{
					"x-death": []any{
						amqp.Table{
							"count":  int32(5),
							"reason": "expired",
							"queue":  "test.queue",
						},
					},
				},
			},
		})

		require.NotNil(t, dlq)
		assert.Equal(t, 5, dlq.DeathCount)
		assert.Equal(t, "expired", dlq.DeathReason)
	})
}

// --- ShouldRetry ---

func TestDLQMessage_ShouldRetry(t *testing.T) {
	t.Run("returns true when below max retries", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 2}

		assert.True(t, dlq.ShouldRetry(5))
		assert.True(t, dlq.ShouldRetry(3))
	})

	t.Run("returns false when at max retries", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 5}

		assert.False(t, dlq.ShouldRetry(5))
	})

	t.Run("returns false when above max retries", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 5}

		assert.False(t, dlq.ShouldRetry(3))
	})

	t.Run("returns false when max retries is zero", func(t *testing.T) {
		dlq := &DLQMessage{RetryCount: 0}

		assert.False(t, dlq.ShouldRetry(0))
	})
}

// --- GetDeathInfo ---

func TestDLQMessage_GetDeathInfo(t *testing.T) {
	t.Run("formats all fields correctly", func(t *testing.T) {
		dlq := &DLQMessage{
			DeathCount:         3,
			DeathReason:        "rejected",
			OriginalQueue:      "orders.queue",
			OriginalExchange:   "orders.exchange",
			OriginalRoutingKey: "order.created",
			RetryCount:         2,
		}

		info := dlq.GetDeathInfo()

		assert.Contains(t, info, "death_count=3")
		assert.Contains(t, info, "reason=rejected")
		assert.Contains(t, info, "queue=orders.queue")
		assert.Contains(t, info, "exchange=orders.exchange")
		assert.Contains(t, info, "routing_key=order.created")
		assert.Contains(t, info, "retry_count=2")
	})

	t.Run("handles zero values", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{Delivery: amqp.Delivery{}},
		}

		info := dlq.GetDeathInfo()

		assert.Contains(t, info, "death_count=0")
		assert.Contains(t, info, "retry_count=0")
	})
}

// --- ResetRetryCount ---

func TestDLQMessage_ResetRetryCount(t *testing.T) {
	t.Run("returns new instance with retry count zero", func(t *testing.T) {
		original := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{
					Headers: amqp.Table{"x-retry-count": int32(5)},
				},
			},
			RetryCount: 5,
		}

		reset := original.ResetRetryCount()

		assert.Equal(t, 0, reset.RetryCount)
		assert.Equal(t, 5, original.RetryCount)
		assert.NotSame(t, original, reset)
	})
}

// --- GetOriginalDestination ---

func TestDLQMessage_GetOriginalDestination(t *testing.T) {
	t.Run("returns OriginalQueue when set", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{Delivery: amqp.Delivery{}},
			OriginalQueue:  "orders.queue",
		}

		assert.Equal(t, "orders.queue", dlq.GetOriginalDestination())
	})

	t.Run("falls back to x-first-death-queue header", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{
					Headers: amqp.Table{"x-first-death-queue": "fallback.queue"},
				},
			},
		}

		assert.Equal(t, "fallback.queue", dlq.GetOriginalDestination())
	})

	t.Run("returns empty string when neither is set", func(t *testing.T) {
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{Headers: amqp.Table{}},
			},
		}

		assert.Empty(t, dlq.GetOriginalDestination())
	})
}

// --- Body ---

func TestDLQMessage_Body(t *testing.T) {
	t.Run("returns body from embedded MessageContext", func(t *testing.T) {
		body := []byte("test message body")
		dlq := &DLQMessage{
			MessageContext: &MessageContext{
				Delivery: amqp.Delivery{Body: body},
			},
		}

		assert.Equal(t, body, dlq.Body())
	})
}
