package broker

import (
	"sync"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/circuitbreaker"
	"github.com/edaniel30/rabbitmq-kit-go/router"
)

// Consumer is a consumer of messages from a queue.
type Consumer struct {
	client         *Client
	publisher      *Publisher
	mu             sync.RWMutex
	router         *router.Router
	circuitBreaker *circuitbreaker.CircuitBreaker
}

// NewConsumer creates a new Consumer with the given client.
func NewConsumer(client *Client, publisher *Publisher, router *router.Router) *Consumer {
	c := &Consumer{
		client:    client,
		publisher: publisher,
		router:    router,
	}

	// Initialize circuit breaker if enabled
	if client.config.CircuitBreakerEnabled {
		cbConfig := circuitbreaker.Config{
			MaxFailures:         client.config.CircuitBreakerMaxFailures,
			ResetTimeout:        client.config.CircuitBreakerResetTimeout,
			HalfOpenMaxRequests: client.config.CircuitBreakerHalfOpenRequests,
			OnStateChange: func(from, to circuitbreaker.State) {
				client.config.Logger.Warn("Consumer: Circuit breaker state changed from %s to %s", from, to)
			},
		}
		c.circuitBreaker = circuitbreaker.New(cbConfig)
	}

	return c
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
	retryHandler := NewHandler(c.publisher, c.client.config.MaxRetries)

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for delivery := range deliveries {
				messageContext := &router.MessageContext{
					Delivery: delivery,
				}

				// Check circuit breaker if enabled
				if c.circuitBreaker != nil && !c.circuitBreaker.AllowRequest() {
					metrics := c.circuitBreaker.GetMetrics()
					c.client.config.Logger.Warn("Consumer Worker %d: Circuit breaker is %s, rejecting message", workerID, metrics.State)
					// Nack without requeue - let DLX handle it or discard
					messageContext.Nack(false)
					continue
				}

				// Call user handler
				serviceHandler := c.router.GetHandler(messageContext.GetType())
				if serviceHandler == nil {
					c.client.config.Logger.Warn("Consumer Worker %d: No handler found for message type: %s", workerID, messageContext.GetType())
					messageContext.Ack()
					continue
				}

				if err := serviceHandler.Execute(messageContext); err != nil {
					c.client.config.Logger.Error("Consumer Worker %d: Handler error: %v", workerID, err)

					// Record failure in circuit breaker
					if c.circuitBreaker != nil {
						c.circuitBreaker.RecordFailure()
					}

					// Try to retry the message
					retryCtx, cancel := c.client.NewContext()
					retryErr := retryHandler.Retry(retryCtx, delivery)
					cancel()

					if retryErr == errors.ErrMaxRetriesExceeded {
						c.client.config.Logger.Warn("Consumer Worker %d: Max retries exceeded, sending to DLQ", workerID)
						messageContext.Nack(false) // Nack without requeue (will go to DLX if configured)
					} else if retryErr != nil {
						c.client.config.Logger.Error("Consumer Worker %d: Retry failed: %v, nacking message", workerID, retryErr)
						messageContext.Nack(false) // Nack without requeue
					} else {
						// Successfully requeued, ack the original
						messageContext.Ack()
					}
				} else {
					// Success, acknowledge message
					if ackErr := messageContext.Ack(); ackErr != nil {
						c.client.config.Logger.Error("Consumer Worker %d: Failed to ack message: %v", workerID, ackErr)
					}

					// Record success in circuit breaker
					if c.circuitBreaker != nil {
						c.circuitBreaker.RecordSuccess()
					}
				}
			}
		}(i)
	}

	return nil
}

// GetCircuitBreakerMetrics returns the current circuit breaker metrics.
//
// Returns nil if circuit breaker is not enabled.
//
// Example:
//
//	metrics := consumer.GetCircuitBreakerMetrics()
//	if metrics != nil {
//	    log.Printf("Circuit breaker: %s, failures: %d", metrics.State, metrics.Failures)
//	}
func (c *Consumer) GetCircuitBreakerMetrics() *circuitbreaker.Metrics {
	if c.circuitBreaker == nil {
		return nil
	}

	metrics := c.circuitBreaker.GetMetrics()
	return &metrics
}

// ResetCircuitBreaker manually resets the circuit breaker to closed state.
//
// This should be used cautiously, typically only for manual intervention
// or testing purposes.
//
// Returns false if circuit breaker is not enabled.
func (c *Consumer) ResetCircuitBreaker() bool {
	if c.circuitBreaker == nil {
		return false
	}

	c.circuitBreaker.Reset()
	return true
}

