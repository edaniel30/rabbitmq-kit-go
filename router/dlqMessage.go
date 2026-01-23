package router

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DLQMessage represents a message from a Dead Letter Queue with additional metadata.
//
// This provides access to the original message along with information about
// why it was sent to the DLQ (retry count, original queue, death reason, etc.).
//
// Example:
//
//	dlqMsg := router.NewDLQMessage(messageContext)
//	log.Printf("Failed message from queue: %s, reason: %s, retries: %d",
//	    dlqMsg.OriginalQueue, dlqMsg.DeathReason, dlqMsg.RetryCount)
type DLQMessage struct {
	*MessageContext

	// OriginalQueue is the name of the queue where the message originally failed
	OriginalQueue string

	// OriginalExchange is the exchange the message was published to
	OriginalExchange string

	// OriginalRoutingKey is the routing key used when publishing
	OriginalRoutingKey string

	// RetryCount is the number of times the message was retried before DLQ
	RetryCount int

	// DeathReason contains information about why the message was moved to DLQ
	// (e.g., "rejected", "expired", "maxlen")
	DeathReason string

	// DeathCount is the number of times this message has been through DLX
	DeathCount int

	// FirstDeathTimestamp is when the message first entered the DLQ
	FirstDeathTimestamp int64
}

// NewDLQMessage creates a DLQMessage from a MessageContext by extracting
// DLQ-specific headers from the x-death header.
//
// The x-death header is automatically added by RabbitMQ when a message is
// routed through a Dead Letter Exchange.
//
// Example:
//
//	func handleDLQMessage(ctx *router.MessageContext) error {
//	    dlqMsg := router.NewDLQMessage(ctx)
//	    log.Printf("Processing DLQ message: %s", dlqMsg.GetDeathInfo())
//	    return nil
//	}
func NewDLQMessage(ctx *MessageContext) *DLQMessage {
	dlqMsg := &DLQMessage{
		MessageContext: ctx,
		RetryCount:     ctx.GetRetryCount(),
	}

	// Extract x-death information if present
	if xDeath, ok := ctx.Delivery.Headers["x-death"].([]interface{}); ok && len(xDeath) > 0 {
		if firstDeath, ok := xDeath[0].(amqp.Table); ok {
			// Extract original queue
			if queue, ok := firstDeath["queue"].(string); ok {
				dlqMsg.OriginalQueue = queue
			}

			// Extract original exchange
			if exchange, ok := firstDeath["exchange"].(string); ok {
				dlqMsg.OriginalExchange = exchange
			}

			// Extract original routing keys
			if routingKeys, ok := firstDeath["routing-keys"].([]interface{}); ok && len(routingKeys) > 0 {
				if rk, ok := routingKeys[0].(string); ok {
					dlqMsg.OriginalRoutingKey = rk
				}
			}

			// Extract death reason
			if reason, ok := firstDeath["reason"].(string); ok {
				dlqMsg.DeathReason = reason
			}

			// Extract death count
			if count, ok := firstDeath["count"].(int64); ok {
				dlqMsg.DeathCount = int(count)
			} else if count, ok := firstDeath["count"].(int32); ok {
				dlqMsg.DeathCount = int(count)
			}

			// Extract first death timestamp
			if timestamp, ok := firstDeath["time"].(time.Time); ok {
				dlqMsg.FirstDeathTimestamp = timestamp.Unix()
			}
		}
	}

	return dlqMsg
}

// ShouldRetry determines if this message should be retried based on retry count.
//
// Returns true if the message has not exceeded the maximum retry limit.
//
// Example:
//
//	if dlqMsg.ShouldRetry(5) {
//	    // Re-enqueue the message
//	    eventBus.RequeueFromDLQ(ctx, dlqMsg)
//	} else {
//	    log.Printf("Message has exceeded max retries: %d", dlqMsg.RetryCount)
//	}
func (d *DLQMessage) ShouldRetry(maxRetries int) bool {
	return d.RetryCount < maxRetries
}

// GetDeathInfo returns a human-readable string with DLQ metadata.
//
// Example output:
//
//	DLQ Message: queue=orders.queue, exchange=orders.exchange, routing_key=order.created,
//	             reason=rejected, retry_count=3, death_count=1
func (d *DLQMessage) GetDeathInfo() string {
	return fmt.Sprintf(
		"DLQ Message: queue=%s, exchange=%s, routing_key=%s, reason=%s, retry_count=%d, death_count=%d",
		d.OriginalQueue,
		d.OriginalExchange,
		d.OriginalRoutingKey,
		d.DeathReason,
		d.RetryCount,
		d.DeathCount,
	)
}

// ResetRetryCount returns a copy of the message with retry count set to zero.
//
// This is useful when re-enqueuing a message from DLQ and you want to give it
// a fresh start with full retry attempts.
func (d *DLQMessage) ResetRetryCount() *DLQMessage {
	copy := *d
	copy.RetryCount = 0
	return &copy
}

// GetOriginalDestination returns the original queue name where this message failed.
//
// Returns empty string if the information is not available.
func (d *DLQMessage) GetOriginalDestination() string {
	if d.OriginalQueue != "" {
		return d.OriginalQueue
	}
	// Fallback: try to extract from headers
	if queue, ok := d.Delivery.Headers["x-first-death-queue"].(string); ok {
		return queue
	}
	return ""
}
