package broker

import (
	"context"
	"log"
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
		conn.Close()
		return errors.NewConnectionError("open channel", err)
	}

	c.conn = conn
	c.channel = ch

	// Setup QoS (prefetch)
	if err := ch.Qos(c.config.PrefetchCount, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return errors.NewConnectionError("set qos", err)
	}

	// Setup topology (exchanges, queues, bindings)
	if err := c.setupTopology(); err != nil {
		ch.Close()
		conn.Close()
		return err
	}

	// Setup close notification for reconnection
	closeErrChan := make(chan *amqp.Error)
	c.conn.NotifyClose(closeErrChan)

	go func() {
		err := <-closeErrChan
		if err != nil && !c.closed {
			log.Printf("[RabbitMQ] Connection closed: %v", err)
			c.done <- true
		}
	}()

	return nil
}

// setupTopology declares exchanges, queues, and bindings.
func (c *Client) setupTopology() error {
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
		// Declare queue
		_, err := c.channel.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false, // no-wait
			q.Args,
		)
		if err != nil {
			return errors.NewTopologyError("declare_queue", q.Name, err)
		}

		// Bind queue to exchange with routing keys
		if q.Exchange != "" {
			for _, routingKey := range q.RoutingKeys {
				err := c.channel.QueueBind(
					q.Name,
					routingKey,
					q.Exchange,
					false, // no-wait
					nil,   // args
				)
				if err != nil {
					return errors.NewTopologyError("bind_queue", q.Name, err)
				}
			}
		}
	}

	return nil
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

		log.Printf("[RabbitMQ] Attempting to reconnect in %v...", c.config.ReconnectDelay)
		time.Sleep(c.config.ReconnectDelay)

		for {
			err := c.connect()
			if err == nil && c.conn != nil && !c.conn.IsClosed() {
				log.Println("[RabbitMQ] Successfully reconnected")
				break
			}

			log.Printf("[RabbitMQ] Reconnection failed: %v. Retrying in %v...", err, c.config.ReconnectDelay)
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
		c.channel.Close()
	}

	if c.conn != nil {
		c.conn.Close()
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

// WithTimeout creates a context with timeout from the given parent context.
func (c *Client) WithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.config.Timeout)
}

// EnsureTimeout ensures the context has a deadline, adding one if needed.
//
// If the context already has a deadline, it returns the context unchanged.
// Otherwise, it adds a timeout using the client's default timeout.
func (c *Client) EnsureTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.config.Timeout)
}
