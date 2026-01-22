package broker

import (
	"context"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	client *Client
}

// NewPublisher creates a new Publisher with the given client.
func NewPublisher(client *Client) *Publisher {
	return &Publisher{
		client: client,
	}
}

// Publish publishes a message to the specified exchange with a routing key.
//
// The message body is sent with content type "application/json" and
// persistent delivery mode.
//
// Example:
//
//	ctx := context.Background()
//	err := client.Publish(ctx, "my.exchange", "routing.key", []byte(`{"foo":"bar"}`))
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	p.client.mu.RLock()
	defer p.client.mu.RUnlock()

	if p.client.closed {
		return errors.ErrClientClosed
	}

	if p.client.channel == nil {
		return errors.ErrNoChannel
	}

	err := p.client.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
		},
	)

	if err != nil {
		return errors.NewPublishError(exchange, routingKey, err)
	}

	return nil
}

// PublishWithOptions publishes a message with custom publishing options.
//
// This allows full control over message properties like headers, priority,
// content type, etc.
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
	p.client.mu.RLock()
	defer p.client.mu.RUnlock()

	if p.client.closed {
		return errors.ErrClientClosed
	}

	if p.client.channel == nil {
		return errors.ErrNoChannel
	}

	err := p.client.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	if err != nil {
		return errors.NewPublishError(exchange, routingKey, err)
	}

	return nil
}
