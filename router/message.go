package router

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// CountHeader is the header key used to track retry attempts
	CountHeader = "x-retry-count"
)

// MessageContext wraps an AMQP delivery with helper methods.
type MessageContext struct {
	Delivery amqp.Delivery
}

// BindJSON unmarshals the message body into the provided value.
func (c *MessageContext) BindJSON(v any) error {
	return json.Unmarshal(c.Delivery.Body, v)
}

// GetType returns the type of the message by parsing the JSON body.
//
// This extracts the "type" field from the message body JSON.
// If parsing fails or the "type" field is missing, it returns an empty string.
//
// Note: This is different from c.Delivery.Type which is the AMQP message type.
func (c *MessageContext) GetType() string {
	var payload struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(c.Delivery.Body, &payload); err != nil {
		// If JSON parsing fails, return empty string
		// This will cause the router to not find a handler and ACK the message
		return ""
	}

	return payload.Type
}

// Ack acknowledges the message.
func (c *MessageContext) Ack() error {
	return c.Delivery.Ack(false)
}

// Nack negatively acknowledges the message.
//
// If requeue is true, the message will be requeued.
// If requeue is false, the message will be discarded or sent to DLX if configured.
func (c *MessageContext) Nack(requeue bool) error {
	return c.Delivery.Nack(false, requeue)
}

// GetHeader retrieves a header value from the message.
func (c *MessageContext) GetHeader(key string) any {
	if c.Delivery.Headers == nil {
		return nil
	}
	return c.Delivery.Headers[key]
}

// GetRetryCount returns the current retry count for this message.
func (c *MessageContext) GetRetryCount() int {
	if c.Delivery.Headers == nil {
		return 0
	}

	rawRetryCount, ok := c.Delivery.Headers[CountHeader]
	if !ok {
		return 0
	}

	// Handle different numeric types
	switch v := rawRetryCount.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// Body returns the raw message body.
func (c *MessageContext) Body() []byte {
	return c.Delivery.Body
}

// GetTraceID returns the trace ID for this message.
func (c *MessageContext) GetTraceID() string {
	trace, ok := c.GetHeader("trace_id").(string)
	if !ok {
		return ""
	}

	return trace
}
