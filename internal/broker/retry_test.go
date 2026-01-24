package broker

import (
	"context"
	"testing"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ShouldRetry(t *testing.T) {
	t.Run("returns true when below max retries", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Headers: amqp.Table{"x-retry-count": int32(2)},
		}

		shouldRetry, count := handler.ShouldRetry(delivery)
		assert.True(t, shouldRetry)
		assert.Equal(t, 2, count)
	})

	t.Run("returns false when at max retries", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Headers: amqp.Table{"x-retry-count": int32(3)},
		}

		shouldRetry, count := handler.ShouldRetry(delivery)
		assert.False(t, shouldRetry)
		assert.Equal(t, 3, count)
	})

	t.Run("returns false when above max retries", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Headers: amqp.Table{"x-retry-count": int32(5)},
		}

		shouldRetry, count := handler.ShouldRetry(delivery)
		assert.False(t, shouldRetry)
		assert.Equal(t, 5, count)
	})

	t.Run("returns true for first attempt", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Headers: amqp.Table{},
		}

		shouldRetry, count := handler.ShouldRetry(delivery)
		assert.True(t, shouldRetry)
		assert.Equal(t, 0, count)
	})

	t.Run("handles zero max retries", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 0)

		delivery := amqp.Delivery{
			Headers: amqp.Table{},
		}

		shouldRetry, count := handler.ShouldRetry(delivery)
		assert.False(t, shouldRetry)
		assert.Equal(t, 0, count)
	})
}

func TestHandler_Retry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("Retry increments count and republishes", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.MaxRetries = 3
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "retry.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Exchange:    "retry.exchange",
			RoutingKey:  "retry.test",
			ContentType: "application/json",
			Body:        []byte(`{"test":"data"}`),
			Headers:     amqp.Table{"x-retry-count": int32(1)},
		}

		err = handler.Retry(context.Background(), delivery)
		assert.NoError(t, err)
	})

	t.Run("Retry returns error when max retries exceeded", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		handler := NewHandler(publisher, 2)

		delivery := amqp.Delivery{
			Exchange:   "test",
			RoutingKey: "test",
			Headers:    amqp.Table{"x-retry-count": int32(3)},
		}

		err = handler.Retry(context.Background(), delivery)
		assert.Error(t, err)
	})

	t.Run("Retry creates headers if nil", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "retry.exchange", Type: "topic", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		handler := NewHandler(publisher, 3)

		delivery := amqp.Delivery{
			Exchange:    "retry.exchange",
			RoutingKey:  "retry.nil",
			ContentType: "application/json",
			Body:        []byte(`{"test":"data"}`),
			Headers:     nil, // No headers
		}

		err = handler.Retry(context.Background(), delivery)
		assert.NoError(t, err)
	})
}

func TestNewHandler(t *testing.T) {
	t.Run("creates handler with parameters", func(t *testing.T) {
		publisher := &Publisher{}
		handler := NewHandler(publisher, 5)

		require.NotNil(t, handler)
		assert.Equal(t, publisher, handler.publisher)
		assert.Equal(t, 5, handler.maxRetries)
	})
}
