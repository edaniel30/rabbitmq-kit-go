package broker

import (
	"context"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublisher_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("Publish without confirms", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = false
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		ctx := context.Background()
		err = publisher.Publish(ctx, "test.exchange", "test.key", []byte(`{"test":"data"}`))
		assert.NoError(t, err)
	})

	t.Run("Publish with publisher confirms", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = true
		cfg.ConfirmTimeout = 5 * time.Second
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		ctx := context.Background()
		err = publisher.Publish(ctx, "test.exchange", "test.key", []byte(`{"test":"confirmed"}`))
		assert.NoError(t, err)
	})

	t.Run("PublishWithOptions with custom publishing", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		ctx := context.Background()
		msg := amqp.Publishing{
			ContentType:  "application/json",
			Body:         []byte(`{"custom":"message"}`),
			DeliveryMode: amqp.Persistent,
			Priority:     5,
			Headers: amqp.Table{
				"x-custom-header": "value",
			},
		}

		err = publisher.PublishWithOptions(ctx, "test.exchange", "test.key", msg)
		assert.NoError(t, err)
	})

	t.Run("PublishBatchPipeline publishes multiple messages", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = true
		cfg.ConfirmTimeout = 10 * time.Second
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		messages := []PublishMessage{
			{Exchange: "test.exchange", RoutingKey: "test.1", Body: []byte(`{"msg":1}`)},
			{Exchange: "test.exchange", RoutingKey: "test.2", Body: []byte(`{"msg":2}`)},
			{Exchange: "test.exchange", RoutingKey: "test.3", Body: []byte(`{"msg":3}`)},
		}

		ctx := context.Background()
		errors, err := publisher.PublishBatchPipeline(ctx, messages)

		require.NoError(t, err)
		assert.Len(t, errors, 3)
		for i, msgErr := range errors {
			assert.NoError(t, msgErr, "message %d should succeed", i)
		}
	})

	t.Run("PublishBatchPipeline without confirms", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = false
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		messages := []PublishMessage{
			{Exchange: "test.exchange", RoutingKey: "test.1", Body: []byte(`{"msg":1}`)},
			{Exchange: "test.exchange", RoutingKey: "test.2", Body: []byte(`{"msg":2}`)},
		}

		ctx := context.Background()
		errors, err := publisher.PublishBatchPipeline(ctx, messages)

		require.NoError(t, err)
		assert.Len(t, errors, 2)
		for _, msgErr := range errors {
			assert.NoError(t, msgErr)
		}
	})

	t.Run("Close stops publisher", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = true

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		publisher.Close()

		// Publisher should be stopped
		assert.False(t, publisher.started)
	})
}

func TestPublisher_Errors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("Publish to closed client returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, _ := New(cfg)
		publisher := NewPublisher(client)
		_ = client.Close()

		err := publisher.Publish(context.Background(), "test", "test", []byte("data"))
		assert.Error(t, err)
	})

	t.Run("PublishBatchPipeline with empty messages", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, _ := New(cfg)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		errors, err := publisher.PublishBatchPipeline(context.Background(), []PublishMessage{})
		assert.NoError(t, err)
		assert.Nil(t, errors)
	})

	t.Run("Publish with cancelled context returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.PublisherConfirms = true
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}

		client, _ := New(cfg)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := publisher.Publish(ctx, "test.exchange", "key", []byte("data"))
		assert.Error(t, err)
	})
}
