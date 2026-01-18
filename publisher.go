package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (b *Broker) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.channel.PublishWithContext(ctx,
		exchange, routingKey, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}
