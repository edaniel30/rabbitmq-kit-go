// Package direct exposes low-level AMQP primitives for callers that need
// fine-grained control over the connection, channels, and topology — outside
// the high-level EventBus.
//
// Use direct when:
//
//   - You need ad-hoc, ephemeral topology (e.g. spy queues with dynamic
//     bindings) that doesn't fit the EventBus's pre-declared QueueConfig model.
//   - You want raw deliveries (RoutingKey, Exchange, Body) without the
//     InternalEvent JSON envelope.
//   - The caller is expected to own and tear down the lifecycle (no automatic
//     reconnection, circuit breaker, or retry).
//
// For standard publish/consume of domain events with reconnection and DLQ
// handling, prefer rabbitmq.NewEventBus instead.
package direct

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// Connect opens a raw AMQP connection to the given URI.
//
// Unlike rabbitmq.NewEventBus, this function returns the underlying amqp091
// connection directly without setting up topology, automatic reconnection, or
// circuit breakers.
//
// The caller owns the connection lifecycle and must Disconnect it when done.
//
// Example:
//
//	conn, err := direct.Connect("amqp://guest:guest@localhost:5672/")
//	if err != nil {
//	    return err
//	}
//	defer direct.Disconnect(conn)
//
//	ch, err := conn.Channel()
//	// ... declare queue, bind, consume, etc.
func Connect(uri string) (*amqp.Connection, error) {
	return amqp.Dial(uri)
}

// Disconnect closes a raw AMQP connection previously returned by Connect.
//
// It is safe to call on a nil connection (returns nil) or on an already-closed
// connection (returns the underlying error from amqp091, which callers can
// ignore at shutdown).
func Disconnect(conn *amqp.Connection) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}
