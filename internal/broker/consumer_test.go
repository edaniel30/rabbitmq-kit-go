package broker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHandler struct {
	mu       sync.Mutex
	messages []string
	err      error
}

func (h *testHandler) Execute(ctx *router.MessageContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	if msg, ok := data["message"].(string); ok {
		h.messages = append(h.messages, msg)
	}

	return h.err
}

func (h *testHandler) GetMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.messages...)
}

func TestConsumer_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	setupClientWithQueue := func(queueName string) (*Client, *Publisher, *router.Router) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}
		cfg.Queues = []config.QueueConfig{
			{
				Name:        queueName,
				Durable:     true,
				Exchange:    "test.exchange",
				RoutingKeys: []string{"test.#"},
			},
		}

		client, err := New(cfg)
		require.NoError(t, err)

		publisher := NewPublisher(client)
		r := router.NewRouter()

		return client, publisher, r
	}

	t.Run("Consume processes messages successfully", func(t *testing.T) {
		client, publisher, r := setupClientWithQueue("consumer.test.queue")
		defer func() { _ = client.Close() }()
		defer publisher.Close()

		handler := &testHandler{}
		r.Handle("test.event", handler)

		consumer := NewConsumer(client, publisher, r)

		// Start consumer in background
		go func() { _ = consumer.Consume("consumer.test.queue", 2) }()
		time.Sleep(100 * time.Millisecond)

		// Publish a message
		msg := `{"type":"test.event","message":"hello consumer"}`
		err := publisher.Publish(context.Background(), "test.exchange", "test.event", amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(msg),
		})
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(150 * time.Millisecond)

		messages := handler.GetMessages()
		assert.Contains(t, messages, "hello consumer")
	})

	t.Run("Consume with multiple workers", func(t *testing.T) {
		client, publisher, r := setupClientWithQueue("consumer.multi.queue")
		defer func() { _ = client.Close() }()
		defer publisher.Close()

		handler := &testHandler{}
		r.Handle("test.multi", handler)

		consumer := NewConsumer(client, publisher, r)

		go func() { _ = consumer.Consume("consumer.multi.queue", 3) }()
		time.Sleep(100 * time.Millisecond)

		// Publish multiple messages
		for i := 0; i < 5; i++ {
			msg := `{"type":"test.multi","message":"msg"}`
			_ = publisher.Publish(context.Background(), "test.exchange", "test.multi", amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(msg),
			})
		}

		time.Sleep(200 * time.Millisecond)

		messages := handler.GetMessages()
		assert.GreaterOrEqual(t, len(messages), 5)
	})

	t.Run("GetCircuitBreakerMetrics without circuit breaker", func(t *testing.T) {
		client, publisher, r := setupClientWithQueue("consumer.cb.queue")
		defer func() { _ = client.Close() }()
		defer publisher.Close()

		consumer := NewConsumer(client, publisher, r)

		metrics := consumer.GetCircuitBreakerMetrics()
		assert.Nil(t, metrics)
	})

	t.Run("ResetCircuitBreaker without circuit breaker", func(t *testing.T) {
		client, publisher, r := setupClientWithQueue("consumer.reset.queue")
		defer func() { _ = client.Close() }()
		defer publisher.Close()

		consumer := NewConsumer(client, publisher, r)

		result := consumer.ResetCircuitBreaker()
		assert.False(t, result)
	})

	t.Run("Consume with circuit breaker enabled", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.CircuitBreakerEnabled = true
		cfg.CircuitBreakerMaxFailures = 2
		cfg.CircuitBreakerResetTimeout = 5 * time.Second
		cfg.Queues = []config.QueueConfig{
			{Name: "consumer.cb.enabled.queue", Durable: true, Exchange: "test.exchange", RoutingKeys: []string{"cb.#"}},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		defer publisher.Close()

		r := router.NewRouter()
		handler := &testHandler{err: assert.AnError} // Handler that always fails
		r.Handle("cb.event", handler)

		consumer := NewConsumer(client, publisher, r)

		go func() { _ = consumer.Consume("consumer.cb.enabled.queue", 1) }()
		time.Sleep(200 * time.Millisecond)

		// Publish messages that will fail
		for i := 0; i < 3; i++ {
			msg := `{"type":"cb.event","message":"fail"}`
			_ = publisher.Publish(context.Background(), "test.exchange", "cb.fail", amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(msg),
			})
			time.Sleep(100 * time.Millisecond)
		}

		time.Sleep(150 * time.Millisecond)

		// Circuit breaker should now have metrics
		metrics := consumer.GetCircuitBreakerMetrics()
		require.NotNil(t, metrics)

		// Reset circuit breaker
		result := consumer.ResetCircuitBreaker()
		assert.True(t, result)
	})
}

func TestConsumer_Errors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("Consume on closed client returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, _ := New(cfg)
		publisher := NewPublisher(client)
		r := router.NewRouter()

		consumer := NewConsumer(client, publisher, r)

		_ = client.Close()

		err := consumer.Consume("test.queue", 1)
		assert.Error(t, err)
	})

	t.Run("Consume on non-existent queue returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, _ := New(cfg)
		defer func() { _ = client.Close() }()

		publisher := NewPublisher(client)
		r := router.NewRouter()

		consumer := NewConsumer(client, publisher, r)

		err := consumer.Consume("nonexistent.queue", 1)
		assert.Error(t, err)
	})
}
