package broker

import (
	"log"
	"sync"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/router"
)

// Consumer is a consumer of messages from a queue.
type Consumer struct {
	client    *Client
	publisher *Publisher
	mu        sync.RWMutex
	router    *router.Router
}

// NewConsumer creates a new Consumer with the given client.
func NewConsumer(client *Client, publisher *Publisher, router *router.Router) *Consumer {
	return &Consumer{
		client:    client,
		publisher: publisher,
		router:    router,
	}
}

// Consume starts consuming messages from a queue with multiple workers.
//
// The handler function will be called for each message. If the handler returns
// an error, the message will be retried according to the client's MaxRetries
// configuration. After max retries, the message will be acknowledged and discarded
// (or sent to DLX if configured).
//
// Parameters:
//   - queue: name of the queue to consume from
//   - workers: number of concurrent workers to process messages
//   - handler: function to handle each message
//
// Example:
//
//	err := client.Consume("my.queue", 5, func(ctx *rabbitmq.MessageContext) error {
//	    var msg MyMessage
//	    if err := ctx.BindJSON(&msg); err != nil {
//	        return err
//	    }
//	    // Process message...
//	    return nil
//	})
func (c *Consumer) Consume(queue string, workers int) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.client.closed {
		return errors.ErrClientClosed
	}

	if c.client.channel == nil {
		return errors.ErrNoChannel
	}

	// Start consuming
	deliveries, err := c.client.channel.Consume(
		queue,
		"",    // consumer tag (auto-generated)
		false, // auto-ack (manual ack for retry logic)
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.NewConsumeError(queue, err)
	}

	// Create retry handler
	retryHandler := NewHandler(*c.publisher, c.client.config.MaxRetries)

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for delivery := range deliveries {
				messageContext := &router.MessageContext{
					Delivery: delivery,
				}

				// Call user handler
				serviceHandler := c.router.GetHandler(messageContext.GetType())
				if serviceHandler == nil {
					log.Printf("[RabbitMQ Worker %d] No handler found for message type: %s", workerID, messageContext.GetType())
					messageContext.Ack()
					continue
				}

				if err := serviceHandler.Execute(messageContext); err != nil {
					log.Printf("[RabbitMQ Worker %d] Handler error: %v", workerID, err)

					// Try to retry the message
					retryCtx, cancel := c.client.NewContext()
					retryErr := retryHandler.Retry(retryCtx, delivery)
					cancel()

					if retryErr == ErrMaxRetriesExceeded {
						log.Printf("[RabbitMQ Worker %d] Max retries exceeded, discarding message", workerID)
						messageContext.Ack() // Ack to remove from queue (will go to DLX if configured)
					} else if retryErr != nil {
						log.Printf("[RabbitMQ Worker %d] Retry failed: %v, nacking message", workerID, retryErr)
						messageContext.Nack(false) // Nack without requeue
					} else {
						// Successfully requeued, ack the original
						messageContext.Ack()
					}
				} else {
					// Success, acknowledge message
					if ackErr := messageContext.Ack(); ackErr != nil {
						log.Printf("[RabbitMQ Worker %d] Failed to ack message: %v", workerID, ackErr)
					}
				}
			}
		}(i)
	}

	return nil
}
