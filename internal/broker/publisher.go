package broker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	client *Client

	// High-performance publisher confirms
	confirms    chan amqp.Confirmation
	pending     map[uint64]chan bool
	mu          sync.RWMutex
	deliveryTag uint64
	done        chan struct{}
	started     bool
	channelID   uintptr // Track which channel we're registered with
}

// NewPublisher creates a new Publisher with the given client.
//
// If PublisherConfirms is enabled in the config, this will setup a
// high-performance asynchronous confirmation system that can handle
// thousands of messages per second.
func NewPublisher(client *Client) *Publisher {
	p := &Publisher{
		client:  client,
		pending: make(map[uint64]chan bool),
		done:    make(chan struct{}),
	}

	// Setup high-performance confirms if enabled
	p.client.mu.RLock()
	confirmsEnabled := p.client.config.PublisherConfirms
	if confirmsEnabled && p.client.channel != nil {
		// Store initial channel ID
		p.channelID = uintptr(unsafe.Pointer(p.client.channel))
	}
	p.client.mu.RUnlock()

	if confirmsEnabled {
		p.setupConfirms()
	}

	return p
}

// setupConfirms initializes the asynchronous confirmation system.
//
// This creates a single shared confirmation channel and starts a
// background goroutine that processes all confirmations, mapping
// them back to the correct pending publish operation.
func (p *Publisher) setupConfirms() {
	p.client.mu.RLock()
	defer p.client.mu.RUnlock()

	if p.client.channel == nil {
		return
	}

	// Create shared confirmation channel
	p.confirms = p.client.channel.NotifyPublish(make(chan amqp.Confirmation, 100))
	p.started = true

	// Start confirmation processor
	go p.processConfirmations()

	p.client.config.Logger.Info("Publisher: High-performance confirms enabled")
}

// ensureConfirmsSetup checks if confirms are setup for the current channel.
// If the channel has been recreated (reconnection), this will reset the
// delivery tag counter and re-setup confirms.
func (p *Publisher) ensureConfirmsSetup() {
	p.client.mu.RLock()
	currentChannelID := uintptr(0)
	if p.client.channel != nil {
		// Use channel pointer as unique ID
		currentChannelID = uintptr(unsafe.Pointer(p.client.channel))
	}
	confirmsEnabled := p.client.config.PublisherConfirms
	p.client.mu.RUnlock()

	if !confirmsEnabled {
		return
	}

	// Check if channel has changed (reconnection)
	p.mu.Lock()
	needsReset := currentChannelID != p.channelID
	oldDone := p.done

	if needsReset {
		// Channel has changed - prepare for reset
		if p.started {
			// Create new done channel before stopping old processor
			p.done = make(chan struct{})
			p.started = false
		}

		// Reset delivery tag counter to 0 (next publish will be tag 1)
		atomic.StoreUint64(&p.deliveryTag, 0)

		// Clear pending map
		for _, resultChan := range p.pending {
			close(resultChan)
		}
		p.pending = make(map[uint64]chan bool)

		// Update channel ID
		p.channelID = currentChannelID
	}
	p.mu.Unlock()

	// Stop old processor AFTER releasing lock to avoid deadlock
	if needsReset && oldDone != nil {
		close(oldDone)
		// Give old goroutine time to exit
		time.Sleep(10 * time.Millisecond)
	}

	// Re-setup confirms with new channel
	if needsReset {
		p.client.mu.RLock()
		if p.client.channel != nil {
			p.mu.Lock()
			p.confirms = p.client.channel.NotifyPublish(make(chan amqp.Confirmation, 100))
			p.started = true
			p.mu.Unlock()

			go p.processConfirmations()
			p.client.config.Logger.Info("Publisher: Confirms re-initialized after reconnection")
		}
		p.client.mu.RUnlock()
	}
}

// processConfirmations runs in a background goroutine and processes
// all confirmations from RabbitMQ, routing them to the correct
// waiting publish operation.
func (p *Publisher) processConfirmations() {
	for {
		select {
		case confirmation, ok := <-p.confirms:
			if !ok {
				// Channel closed, clean up all pending
				p.mu.Lock()
				for _, resultChan := range p.pending {
					close(resultChan)
				}
				p.pending = make(map[uint64]chan bool)
				p.mu.Unlock()
				return
			}

			// Route confirmation to waiting publisher
			p.mu.Lock()
			if resultChan, exists := p.pending[confirmation.DeliveryTag]; exists {
				resultChan <- confirmation.Ack
				close(resultChan)
				delete(p.pending, confirmation.DeliveryTag)
			} else {
				p.client.config.Logger.Warn("Publisher: Received unexpected confirmation for delivery tag %d", confirmation.DeliveryTag)
			}
			p.mu.Unlock()

		case <-p.done:
			return
		}
	}
}

// Close stops the confirmation processor and cleans up resources.
func (p *Publisher) Close() {
	if p.started {
		close(p.done)
		p.started = false
	}
}

// createDefaultPublishing creates a standard AMQP Publishing message with default settings.
//
// Default settings:
//   - ContentType: "application/json"
//   - DeliveryMode: Persistent
//   - Timestamp: Current time
func createDefaultPublishing(body []byte) amqp.Publishing {
	return amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
	}
}

// Publish publishes a message to the specified exchange with a routing key.
//
// The message body is sent with content type "application/json" and
// persistent delivery mode.
//
// If PublisherConfirms is enabled in the config, this method will wait for
// confirmation from RabbitMQ before returning.
//
// Example:
//
//	ctx := context.Background()
//	err := client.Publish(ctx, "my.exchange", "routing.key", []byte(`{"foo":"bar"}`))
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	return p.PublishWithOptions(ctx, exchange, routingKey, createDefaultPublishing(body))
}

// PublishWithOptions publishes a message with custom publishing options.
//
// This allows full control over message properties like headers, priority,
// content type, etc.
//
// If PublisherConfirms is enabled, this method uses a high-performance
// asynchronous confirmation system that allows publishing thousands of
// messages per second. Each message is tracked by its delivery tag and
// confirmed independently by a background goroutine.
//
// The method will block until:
//   - RabbitMQ confirms the message (ACK/NACK)
//   - ConfirmTimeout expires
//   - Context is cancelled
//
// Example:
//
//	err := client.PublishWithOptions(ctx, "exchange", "key", amqp.Publishing{
//	    ContentType:  "application/json",
//	    Body:         []byte(`{"data":"value"}`),
//	    DeliveryMode: amqp.Persistent,
//	    Priority:     5,
//	    Headers: amqp.Table{
//	        "x-retry-count": 0,
//	    },
//	})
func (p *Publisher) PublishWithOptions(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	// Ensure confirms are setup (handles reconnection)
	p.ensureConfirmsSetup()

	// Check if client is closed
	p.client.mu.RLock()
	if p.client.closed {
		p.client.mu.RUnlock()
		return errors.ErrClientClosed
	}

	if p.client.channel == nil {
		p.client.mu.RUnlock()
		return errors.ErrNoChannel
	}

	confirmsEnabled := p.client.config.PublisherConfirms
	confirmTimeout := p.client.config.ConfirmTimeout
	p.client.mu.RUnlock()

	// Register this message for confirmation if enabled
	var resultChan chan bool
	var deliveryTag uint64

	if confirmsEnabled {
		// Increment delivery tag atomically
		deliveryTag = atomic.AddUint64(&p.deliveryTag, 1)

		// Create result channel for this message
		resultChan = make(chan bool, 1)

		// Register in pending map
		p.mu.Lock()
		p.pending[deliveryTag] = resultChan
		p.mu.Unlock()

		// Cleanup if we don't wait for confirmation (error or timeout)
		defer func() {
			if resultChan != nil {
				p.mu.Lock()
				delete(p.pending, deliveryTag)
				p.mu.Unlock()
			}
		}()
	}

	// Publish the message
	p.client.mu.RLock()
	err := p.client.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
	p.client.mu.RUnlock()

	if err != nil {
		return errors.NewPublishError(exchange, routingKey, err)
	}

	// Wait for confirmation if enabled
	if confirmsEnabled && resultChan != nil {
		select {
		case ack, ok := <-resultChan:
			if !ok {
				// Channel closed by confirmation processor (connection lost)
				p.client.config.Logger.Warn("Publisher: Confirmation channel closed [exchange=%s, routing_key=%s]", exchange, routingKey)
				return errors.ErrPublishConfirmTimeout
			}

			if !ack {
				// NACK received from RabbitMQ
				p.client.config.Logger.Error("Publisher: Message not confirmed (NACK) [exchange=%s, routing_key=%s, delivery_tag=%d]", exchange, routingKey, deliveryTag)
				return errors.ErrPublishNotConfirmed
			}

			// ACK received - message confirmed successfully
			resultChan = nil // Prevent defer cleanup
			return nil

		case <-time.After(confirmTimeout):
			p.client.config.Logger.Error("Publisher: Confirmation timeout [exchange=%s, routing_key=%s, delivery_tag=%d, timeout=%v]", exchange, routingKey, deliveryTag, confirmTimeout)
			return errors.ErrPublishConfirmTimeout

		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// PublishMessage represents a message to be published in a batch.
type PublishMessage struct {
	Exchange   string
	RoutingKey string
	Body       []byte
}

// PublishBatchPipeline publishes multiple messages using pipelining for maximum throughput.
//
// This method implements true AMQP pipelining by:
//  1. Sending ALL messages without waiting for confirmations
//  2. Collecting all delivery tags
//  3. Waiting for all confirmations in batch
//
// This is 5-10x faster than sequential publishing for large batches.
//
// Returns a slice of errors (one per message). If a message succeeds, its error is nil.
// If Publisher Confirms is disabled, all errors will be nil.
//
// Example:
//
//	messages := []PublishMessage{
//	    {Exchange: "orders", RoutingKey: "order.created", Body: json1},
//	    {Exchange: "orders", RoutingKey: "order.updated", Body: json2},
//	}
//	errors, err := publisher.PublishBatchPipeline(ctx, messages)
//	if err != nil {
//	    // Fatal error (connection lost, etc.)
//	    return err
//	}
//	// Check individual message errors
//	for i, msgErr := range errors {
//	    if msgErr != nil {
//	        log.Printf("Message %d failed: %v", i, msgErr)
//	    }
//	}
func (p *Publisher) PublishBatchPipeline(ctx context.Context, messages []PublishMessage) ([]error, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	// Ensure confirms are setup (handles reconnection)
	p.ensureConfirmsSetup()

	// Check if client is closed
	p.client.mu.RLock()
	if p.client.closed {
		p.client.mu.RUnlock()
		return nil, errors.ErrClientClosed
	}

	if p.client.channel == nil {
		p.client.mu.RUnlock()
		return nil, errors.ErrNoChannel
	}

	confirmsEnabled := p.client.config.PublisherConfirms
	confirmTimeout := p.client.config.ConfirmTimeout
	p.client.mu.RUnlock()

	// Initialize results
	messageErrors := make([]error, len(messages))

	// If confirms not enabled, just publish all messages
	if !confirmsEnabled {
		for i, msg := range messages {
			p.client.mu.RLock()
			err := p.client.channel.PublishWithContext(
				ctx,
				msg.Exchange,
				msg.RoutingKey,
				false, // mandatory
				false, // immediate
				createDefaultPublishing(msg.Body),
			)
			p.client.mu.RUnlock()

			if err != nil {
				messageErrors[i] = errors.NewPublishError(msg.Exchange, msg.RoutingKey, err)
			}
		}
		return messageErrors, nil
	}

	// PIPELINING: Send all messages first, collect delivery tags
	deliveryTags := make([]uint64, len(messages))
	resultChans := make([]chan bool, len(messages))

	for i, msg := range messages {
		// Increment delivery tag atomically
		deliveryTag := atomic.AddUint64(&p.deliveryTag, 1)
		deliveryTags[i] = deliveryTag

		// Create result channel for this message
		resultChan := make(chan bool, 1)
		resultChans[i] = resultChan

		// Register in pending map
		p.mu.Lock()
		p.pending[deliveryTag] = resultChan
		p.mu.Unlock()

		// Publish message WITHOUT waiting
		p.client.mu.RLock()
		err := p.client.channel.PublishWithContext(
			ctx,
			msg.Exchange,
			msg.RoutingKey,
			false, // mandatory
			false, // immediate
			createDefaultPublishing(msg.Body),
		)
		p.client.mu.RUnlock()

		if err != nil {
			// Publish failed immediately, clean up
			p.mu.Lock()
			delete(p.pending, deliveryTag)
			p.mu.Unlock()
			close(resultChan)
			messageErrors[i] = errors.NewPublishError(msg.Exchange, msg.RoutingKey, err)
		}
	}

	// Now wait for ALL confirmations
	for i, resultChan := range resultChans {
		if messageErrors[i] != nil {
			// Already failed during publish
			continue
		}

		select {
		case ack, ok := <-resultChan:
			if !ok {
				// Channel closed by confirmation processor (connection lost)
				messageErrors[i] = errors.ErrPublishConfirmTimeout
			} else if !ack {
				// NACK received from RabbitMQ
				messageErrors[i] = errors.ErrPublishNotConfirmed
			}
			// else: ACK received, error remains nil

		case <-time.After(confirmTimeout):
			// Timeout waiting for confirmation
			messageErrors[i] = errors.ErrPublishConfirmTimeout
			// Clean up pending
			p.mu.Lock()
			delete(p.pending, deliveryTags[i])
			p.mu.Unlock()

		case <-ctx.Done():
			// Context cancelled
			return messageErrors, ctx.Err()
		}
	}

	return messageErrors, nil
}
