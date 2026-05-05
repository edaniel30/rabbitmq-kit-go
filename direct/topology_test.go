package direct_test

import (
	"context"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/direct"
	rabbitTest "github.com/edaniel30/rabbitmq-kit-go/testing"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeclareTopology_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	container := rabbitTest.SetupRabbitMQContainer(t)
	defer container.Teardown(t)

	conn, err := direct.Connect(container.URI)
	require.NoError(t, err)
	defer func() { _ = direct.Disconnect(conn) }()

	t.Run("declares topic exchange, queue and routes by key", func(t *testing.T) {
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer func() { _ = ch.Close() }()

		exchangeName := "direct.test.topic"
		queueName := "direct.test.queue"

		q, err := direct.DeclareTopology(ch, direct.Topology{
			Exchange: config.ExchangeConfig{
				Name:       exchangeName,
				Type:       "topic",
				AutoDelete: true,
			},
			Queue: config.QueueConfig{
				Name:        queueName,
				RoutingKeys: []string{"order.created", "order.completed"},
				AutoDelete:  true,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, queueName, q.Name)

		// Publish a message matching one of the routing keys and confirm it
		// arrives — proves both declaration and binding succeeded.
		err = ch.Publish(exchangeName, "order.created", false, false, amqp.Publishing{
			Body: []byte(`{"id":"1"}`),
		})
		require.NoError(t, err)

		msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
		require.NoError(t, err)

		select {
		case msg := <-msgs:
			assert.Equal(t, "order.created", msg.RoutingKey)
			assert.Equal(t, `{"id":"1"}`, string(msg.Body))
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for routed message")
		}
	})

	t.Run("empty routing keys leaves queue unbound", func(t *testing.T) {
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer func() { _ = ch.Close() }()

		exchangeName := "direct.test.topic.unbound"
		queueName := "direct.test.queue.unbound"

		q, err := direct.DeclareTopology(ch, direct.Topology{
			Exchange: config.ExchangeConfig{
				Name:       exchangeName,
				Type:       "topic",
				AutoDelete: true,
			},
			Queue: config.QueueConfig{
				Name:        queueName,
				RoutingKeys: nil,
				AutoDelete:  true,
			},
		})
		require.NoError(t, err)
		require.Equal(t, queueName, q.Name)

		// Publishing on the topic exchange must NOT reach the queue, since no
		// binding was created.
		err = ch.Publish(exchangeName, "any.key", false, false, amqp.Publishing{
			Body: []byte(`{"id":"x"}`),
		})
		require.NoError(t, err)

		// Use a short context so we don't block the test if a message ever
		// arrives — but the expectation is that nothing does.
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
		require.NoError(t, err)

		select {
		case msg, ok := <-msgs:
			if ok {
				t.Fatalf("unexpected message delivered to unbound queue: %s", string(msg.Body))
			}
		case <-ctx.Done():
			// expected — no delivery
		}
	})
}
