package broker

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"sync"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/circuitbreaker"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
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
				client.config.Logger.Warn(
					context.Background(),
					"Consumer: Circuit breaker state changed",
					map[string]any{
						"from": from,
						"to":   to,
					},
				)
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
	for i := range workers {
		go func(workerID int) {
			for delivery := range deliveries {
				// Get trace ID from headers, generate one if not present
				traceID, ok := delivery.Headers["trace_id"].(string)
				if !ok {
					if delivery.Headers == nil {
						delivery.Headers = amqp.Table{}
					}
					traceID = uuid.New().String()
					delivery.Headers["trace_id"] = traceID
				}

				bodyJSON := map[string]any{}
				err := json.Unmarshal(delivery.Body, &bodyJSON)
				if err != nil {
					c.client.config.Logger.Error(
						context.Background(),
						"Consumer Worker: Failed to unmarshal event body",
						map[string]any{
							"worker_id": workerID,
							"error":     err,
							"trace_id":  traceID,
						},
					)
				}

				c.client.config.Logger.Debug(
					context.Background(),
					"Consumer Worker: Received event",
					map[string]any{
						"worker_id":  workerID,
						"trace_id":   traceID,
						"event_type": delivery.Type,
						"event":      bodyJSON,
					},
				)

				messageContext := &router.MessageContext{
					Delivery: delivery,
				}

				// Check circuit breaker if enabled
				if c.circuitBreaker != nil && !c.circuitBreaker.AllowRequest() {
					metrics := c.circuitBreaker.GetMetrics()
					c.client.config.Logger.Warn(
						context.Background(),
						"Consumer Worker: Circuit breaker is open, rejecting message",
						map[string]any{
							"worker_id": workerID,
							"state":     metrics.State,
							"trace_id":  traceID,
						},
					)
					// Nack without requeue - let DLX handle it or discard
					_ = messageContext.Nack(false)
					continue
				}

				// Call user handler
				serviceHandler := c.router.GetHandler(messageContext.GetType())
				if serviceHandler == nil {
					c.client.config.Logger.Error(
						context.Background(),
						"Consumer Worker: No handler found for message type",
						map[string]any{
							"worker_id": workerID,
							"type":      messageContext.GetType(),
							"trace_id":  traceID,
						},
					)
					_ = messageContext.Ack()
					continue
				}

				if err := serviceHandler.Execute(messageContext); err != nil {
					c.client.config.Logger.Error(
						context.Background(),
						"Consumer Worker: Handler error",
						map[string]any{
							"worker_id": workerID,
							"trace_id":  messageContext.GetTraceID(),
							"error":     err,
						},
					)

					// Record failure in circuit breaker
					if c.circuitBreaker != nil {
						c.circuitBreaker.RecordFailure()
					}

					// Try to retry the message
					retryCtx, cancel := c.client.NewContext()
					retryErr := retryHandler.Retry(retryCtx, delivery)
					cancel()

					switch {
					case stderrors.Is(retryErr, errors.ErrMaxRetriesExceeded):
						c.client.config.Logger.Warn(
							context.Background(),
							"Consumer Worker: Max retries exceeded, sending to DLQ",
							map[string]any{
								"worker_id": workerID,
								"trace_id":  traceID,
							},
						)
						_ = messageContext.Nack(false) // Nack without requeue (will go to DLX if configured)
					case retryErr != nil:
						c.client.config.Logger.Error(
							context.Background(),
							"Consumer Worker: Retry failed, nacking message",
							map[string]any{
								"worker_id": workerID,
								"error":     retryErr,
								"trace_id":  traceID,
							},
						)
						_ = messageContext.Nack(false) // Nack without requeue
					default:
						// Successfully requeued, ack the original
						retryCount := messageContext.GetRetryCount() + 1
						c.client.config.Logger.Debug(
							context.Background(),
							"Consumer Worker: Message requeued for retry",
							map[string]any{
								"worker_id":   workerID,
								"trace_id":    traceID,
								"retry_count": retryCount,
							},
						)
						_ = messageContext.Ack()
					}
				} else {
					// Success, acknowledge message
					if ackErr := messageContext.Ack(); ackErr != nil {
						c.client.config.Logger.Error(
							context.Background(),
							"Consumer Worker: Failed to ack message",
							map[string]any{
								"worker_id": workerID,
								"error":     ackErr,
								"trace_id":  traceID,
							},
						)
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
