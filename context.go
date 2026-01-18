package rabbitmq

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Context struct {
	Delivery amqp.Delivery
}

func (c *Context) BindJSON(v any) error {
	return json.Unmarshal(c.Delivery.Body, v)
}

func (c *Context) Ack() error {
	return c.Delivery.Ack(false)
}

func (c *Context) Nack(requeue bool) error {
	return c.Delivery.Nack(false, requeue)
}

// GetHeader retrieves useful metadata (like x-retries)
func (c *Context) GetHeader(key string) any {
	return c.Delivery.Headers[key]
}
