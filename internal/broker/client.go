package broker

import (
	"context"
	"sync"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Client is the main RabbitMQ client.
type Client struct {
	config  config.Config
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.RWMutex
	closed  bool
	done    chan bool // Signal channel for reconnection
}

// New creates a new RabbitMQ client with the given configuration.
//
// Example:
//
//	client, err := rabbitmq.New(
//	    rabbitmq.DefaultConfig(),
//	    rabbitmq.WithURI("amqp://guest:guest@localhost:5672/"),
//	    rabbitmq.WithPrefetchCount(10),
//	)
func New(config config.Config) (*Client, error) {
	client := &Client{
		config: config,
		closed: false,
		done:   make(chan bool),
	}

	// Initial connection
	if err := client.connect(); err != nil {
		return nil, err
	}

	// Start reconnection handler
	go client.handleReconnect()

	return client, nil
}

// connect establishes connection to RabbitMQ and sets up topology.
func (c *Client) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return errors.ErrClientClosed
	}

	// Establish connection
	conn, err := amqp.Dial(c.config.URI)
	if err != nil {
		return errors.NewConnectionError("dial", err)
	}

	// Open channel
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return errors.NewConnectionError("open channel", err)
	}

	c.conn = conn
	c.channel = ch

	// Setup QoS (prefetch)
	if err := ch.Qos(c.config.PrefetchCount, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return errors.NewConnectionError("set qos", err)
	}

	// Enable publisher confirms if configured
	if c.config.PublisherConfirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return errors.NewConnectionError("enable publisher confirms", err)
		}
		c.config.Logger.Debug(context.Background(), "Publisher confirms enabled", nil)
	}

	// Setup topology (exchanges, queues, bindings)
	if err := c.setupTopology(); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}

	// Setup close notification for reconnection
	closeErrChan := make(chan *amqp.Error)
	c.conn.NotifyClose(closeErrChan)

	go func() {
		err := <-closeErrChan
		if err != nil && !c.closed {
			c.config.Logger.Warn(context.Background(), "Connection closed unexpectedly", map[string]any{"error": err})
			c.done <- true
		}
	}()

	return nil
}

// setupTopology declares exchanges, queues, and bindings.
func (c *Client) setupTopology() error {
	// Setup DLQ if enabled
	if c.config.DLQEnabled {
		if err := c.setupDLQ(); err != nil {
			return err
		}
	}

	// Declare exchanges
	for _, ex := range c.config.Exchanges {
		err := c.channel.ExchangeDeclare(
			ex.Name,
			ex.Type,
			ex.Durable,
			ex.AutoDelete,
			ex.Internal,
			false, // no-wait
			ex.Args,
		)
		if err != nil {
			return errors.NewTopologyError("declare_exchange", ex.Name, err)
		}
	}

	// Declare queues and bindings
	for _, q := range c.config.Queues {
		// If DLQ is enabled, automatically configure DLX for this queue
		queueConfig := q
		if c.config.DLQEnabled {
			queueConfig = configQueueWithDLX(q, c.config.DLQConfig)
		}

		// Declare queue
		_, err := c.channel.QueueDeclare(
			queueConfig.Name,
			queueConfig.Durable,
			queueConfig.AutoDelete,
			queueConfig.Exclusive,
			false, // no-wait
			queueConfig.Args,
		)
		if err != nil {
			return errors.NewTopologyError("declare_queue", queueConfig.Name, err)
		}

		// Bind queue to exchange with routing keys
		if queueConfig.Exchange != "" {
			for _, routingKey := range queueConfig.RoutingKeys {
				err := c.channel.QueueBind(
					queueConfig.Name,
					routingKey,
					queueConfig.Exchange,
					false, // no-wait
					nil,   // args
				)
				if err != nil {
					return errors.NewTopologyError("bind_queue", queueConfig.Name, err)
				}
			}
		}
	}

	return nil
}

// setupDLQ sets up the Dead Letter Exchange and Dead Letter Queues.
//
// This method:
//  1. Declares the Dead Letter Exchange (DLX)
//  2. Creates a DLQ queue for each configured queue
//  3. Binds each DLQ to the DLX with appropriate routing keys
func (c *Client) setupDLQ() error {
	dlqCfg := c.config.DLQConfig

	// Declare Dead Letter Exchange
	err := c.channel.ExchangeDeclare(
		dlqCfg.ExchangeName,
		dlqCfg.ExchangeType,
		dlqCfg.Durable,
		dlqCfg.AutoDelete,
		false, // internal
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.NewTopologyError("declare_dlx", dlqCfg.ExchangeName, err)
	}

	c.config.Logger.Debug(context.Background(), "Client: DLX exchange declared", map[string]any{
		"exchange": dlqCfg.ExchangeName,
	})

	// Create DLQ queue for each configured queue
	for _, mainQueue := range c.config.Queues {
		dlqName := dlqCfg.GetDLQName(mainQueue.Name)
		routingKey := dlqCfg.GetDLXRoutingKey(mainQueue.Name)

		// Declare DLQ
		_, err := c.channel.QueueDeclare(
			dlqName,
			dlqCfg.Durable,
			dlqCfg.AutoDelete,
			false, // exclusive
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			return errors.NewTopologyError("declare_dlq", dlqName, err)
		}

		// Bind DLQ to DLX
		err = c.channel.QueueBind(
			dlqName,
			routingKey,
			dlqCfg.ExchangeName,
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			return errors.NewTopologyError("bind_dlq", dlqName, err)
		}

		c.config.Logger.Debug(context.Background(), "Client: DLQ configured", map[string]any{
			"queue":       mainQueue.Name,
			"dlq":         dlqName,
			"routing_key": routingKey,
		})
	}

	return nil
}

// configQueueWithDLX configures a queue to use Dead Letter Exchange using WithDLX method.
func configQueueWithDLX(queue config.QueueConfig, dlqCfg config.DLQConfig) config.QueueConfig {
	// Make a copy to avoid modifying the original
	queueCopy := queue
	queueCopy.WithDLX(dlqCfg.ExchangeName, dlqCfg.GetDLXRoutingKey(queue.Name))
	return queueCopy
}

// handleReconnect handles automatic reconnection when connection is lost.
func (c *Client) handleReconnect() {
	for {
		<-c.done

		c.mu.RLock()
		if c.closed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		c.config.Logger.Info(
			context.Background(),
			"Attempting to reconnect in %v...",
			map[string]any{
				"reconnect_delay": c.config.ReconnectDelay,
			},
		)
		time.Sleep(c.config.ReconnectDelay)

		for {
			err := c.connect()
			if err == nil && c.conn != nil && !c.conn.IsClosed() {
				c.config.Logger.Info(
					context.Background(),
					"Successfully reconnected",
					nil,
				)
				break
			}

			c.config.Logger.Error(
				context.Background(),
				"Reconnection failed: %v. Retrying in %v...",
				map[string]any{
					"error":           err,
					"reconnect_delay": c.config.ReconnectDelay,
				},
			)
			time.Sleep(c.config.ReconnectDelay)

			c.mu.RLock()
			if c.closed {
				c.mu.RUnlock()
				return
			}
			c.mu.RUnlock()
		}
	}
}

// GetChannel returns the underlying AMQP channel for advanced operations.
//
// Returns an error if the client is closed or the channel is not available.
//
// This should be used cautiously as it exposes the low-level channel.
func (c *Client) GetChannel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, errors.ErrClientClosed
	}

	if c.channel == nil {
		return nil, errors.ErrNoChannel
	}

	return c.channel, nil
}

// Close gracefully closes the RabbitMQ connection.
//
// This method is idempotent and can be called multiple times safely.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	if c.channel != nil {
		_ = c.channel.Close()
	}

	if c.conn != nil {
		_ = c.conn.Close()
	}

	return nil
}

// IsConnected returns true if the client has an active connection.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn != nil && !c.conn.IsClosed()
}

// NewContext creates a new context with the default timeout from config.
//
// Use this for standalone operations (CLI tools, scripts, background jobs).
// DO NOT use in web handlers - use the request context instead.
func (c *Client) NewContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.config.Timeout)
}
