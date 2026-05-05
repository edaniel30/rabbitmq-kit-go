package direct

import (
	"github.com/edaniel30/rabbitmq-kit-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology bundles an exchange and a queue (with its routing keys) so that
// the full chain — declare exchange, declare queue, bind queue to exchange —
// can be set up in a single call.
type Topology struct {
	Exchange config.ExchangeConfig
	Queue    config.QueueConfig
}

// DeclareTopology declares the exchange, declares the queue, and binds the
// queue to the exchange using Queue.RoutingKeys (one binding per key, or a
// single empty-key binding if RoutingKeys is empty).
//
// The Queue.Exchange field is ignored — bindings always target Exchange.Name
// from the same Topology, so the two cannot drift apart.
//
// Returns the declared amqp.Queue (with broker-assigned name when Queue.Name
// is empty, plus message/consumer counts).
//
// Example:
//
//	conn, _ := direct.Connect(uri)
//	ch, _ := conn.Channel()
//	q, err := direct.DeclareTopology(ch, direct.Topology{
//	    Exchange: config.ExchangeConfig{Name: "orders.events", Type: "topic", Durable: true},
//	    Queue: config.QueueConfig{
//	        Name:        "spy.exec-1.contract",
//	        RoutingKeys: []string{"order.created", "order.completed"},
//	        AutoDelete:  true,
//	        Exclusive:   true,
//	    },
//	})
func DeclareTopology(ch *amqp.Channel, t Topology) (amqp.Queue, error) {
	if err := declareExchange(ch, t.Exchange); err != nil {
		return amqp.Queue{}, err
	}

	q, err := declareQueue(ch, t.Queue)
	if err != nil {
		return amqp.Queue{}, err
	}

	if err := bindQueue(ch, q.Name, t.Exchange.Name, t.Queue.RoutingKeys); err != nil {
		return amqp.Queue{}, err
	}
	return q, nil
}

// declareExchange declares an exchange using config.ExchangeConfig — the same
// struct already used by the EventBus topology setup, so configuration stays
// consistent across the lib.
func declareExchange(ch *amqp.Channel, cfg config.ExchangeConfig) error {
	return ch.ExchangeDeclare(
		cfg.Name,
		cfg.Type,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		false, // no-wait — match the EventBus topology behavior
		amqp.Table(cfg.Args),
	)
}

// declareQueue declares a queue using config.QueueConfig and returns the
// broker's response (which carries the assigned name when cfg.Name is empty).
func declareQueue(ch *amqp.Channel, cfg config.QueueConfig) (amqp.Queue, error) {
	return ch.QueueDeclare(
		cfg.Name,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		false, // no-wait
		amqp.Table(cfg.Args),
	)
}

// bindQueue binds queueName to exchangeName for each routing key. If
// routingKeys is empty, a single empty-key binding is created (fanout-style).
func bindQueue(ch *amqp.Channel, queueName, exchangeName string, routingKeys []string) error {
	if len(routingKeys) == 0 {
		return ch.QueueBind(queueName, "", exchangeName, false, nil)
	}
	for _, rk := range routingKeys {
		if err := ch.QueueBind(queueName, rk, exchangeName, false, nil); err != nil {
			return err
		}
	}
	return nil
}
