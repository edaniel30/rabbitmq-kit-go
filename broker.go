package rabbitmq

import (
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	opts    Options
	conn    *amqp.Connection
	channel *amqp.Channel
	mu      sync.RWMutex
	closed  bool
}

func NewBroker(url string, opts ...Option) *Broker {
	options := defaultOptions()
	options.URL = url
	for _, opt := range opts {
		opt(&options)
	}
	return &Broker{opts: options}
}

func (b *Broker) Connect() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	conn, err := amqp.Dial(b.opts.URL)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	b.conn = conn
	b.channel = ch

	// --- Auto-Setup Logic ---
	if err := b.setupTopology(); err != nil {
		return err
	}

	// QOS: Controls how many messages are sent to workers before receiving an Ack
	_ = ch.Qos(b.opts.PrefetchCount, 0, false)

	go b.handleReconnect()
	return nil
}

// setupTopology creates the exchanges, queues and bindings defined in the options
func (b *Broker) setupTopology() error {
	for _, q := range b.opts.Queues {
		// 1. Declare Exchange (if defined)
		if q.Exchange != "" {
			err := b.channel.ExchangeDeclare(
				q.Exchange, // name
				"direct",   // type (can be made configurable later)
				true,       // durable
				false,      // auto-deleted
				false,      // internal
				false,      // no-wait
				nil,        // arguments
			)
			if err != nil {
				return err
			}
		}

		// 2. Declare Queue
		_, err := b.channel.QueueDeclare(
			q.Name, // name
			true,   // durable
			false,  // delete when unused
			false,  // exclusive
			false,  // no-wait
			nil,    // arguments
		)
		if err != nil {
			return err
		}

		// 3. Bind with Routing Keys (if exchange and keys exist)
		if q.Exchange != "" {
			for _, rk := range q.RoutingKeys {
				err := b.channel.QueueBind(
					q.Name,
					rk,
					q.Exchange,
					false,
					nil,
				)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (b *Broker) handleReconnect() {
	notifyClose := b.conn.NotifyClose(make(chan *amqp.Error))

	err := <-notifyClose
	if err != nil && !b.closed {
		log.Printf("[RabbitMQ] Connection lost. Retrying in %v...", b.opts.ReconnectDelay)
		time.Sleep(b.opts.ReconnectDelay)
		if err := b.Connect(); err != nil {
			log.Printf("[RabbitMQ] Reconnect failed: %v", err)
			b.handleReconnect()
		}
	}
}

func (b *Broker) Close() {
	b.closed = true
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		b.conn.Close()
	}
}
