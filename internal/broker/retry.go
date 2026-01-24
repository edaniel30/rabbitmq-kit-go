package broker

import (
	"context"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler handles message retry logic.
type Handler struct {
	publisher  *Publisher
	maxRetries int
}

// NewHandler creates a new retry handler.
func NewHandler(publisher *Publisher, maxRetries int) *Handler {
	return &Handler{
		publisher:  publisher,
		maxRetries: maxRetries,
	}
}

// ShouldRetry checks if a message should be retried based on retry count.
//
// Returns:
//   - shouldRetry: true if the message should be retried
//   - retryCount: current retry attempt count
func (h *Handler) ShouldRetry(delivery amqp.Delivery) (shouldRetry bool, retryCount int) {
	messageContext := &router.MessageContext{
		Delivery: delivery,
	}

	retryCount = messageContext.GetRetryCount()

	if retryCount >= h.maxRetries {
		return false, retryCount
	}

	return true, retryCount
}

// Retry republishes a failed message with incremented retry count.
//
// This method:
//  1. Increments the x-retry-count header
//  2. Preserves all original message properties
//  3. Sets DeliveryMode to Persistent
//  4. Updates the timestamp
//
// Returns an error if max retries exceeded or publish fails.
func (h *Handler) Retry(ctx context.Context, delivery amqp.Delivery) error {
	shouldRetry, retryCount := h.ShouldRetry(delivery)

	if !shouldRetry {
		return errors.ErrMaxRetriesExceeded
	}

	// Increment retry count
	retryCount++

	// Prepare headers
	headers := delivery.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers[router.CountHeader] = retryCount

	// Republish message with updated retry count
	msg := amqp.Publishing{
		Headers:       headers,
		ContentType:   delivery.ContentType,
		Body:          delivery.Body,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
		CorrelationId: delivery.CorrelationId,
		MessageId:     delivery.MessageId,
		Type:          delivery.Type,
		Priority:      delivery.Priority,
		Expiration:    delivery.Expiration,
		ReplyTo:       delivery.ReplyTo,
		AppId:         delivery.AppId,
		UserId:        delivery.UserId,
	}

	return h.publisher.PublishWithOptions(ctx, delivery.Exchange, delivery.RoutingKey, msg)
}
