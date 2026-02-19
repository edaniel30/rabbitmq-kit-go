package broker

import (
	"os"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	rabbitTest "github.com/edaniel30/rabbitmq-kit-go/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Shared container for all integration tests in this package
var sharedContainer *rabbitTest.RabbitMQContainer

// TestMain sets up a single RabbitMQ container for all tests
func TestMain(m *testing.M) {
	// Setup shared container (will be used only by integration tests)
	sharedContainer = rabbitTest.SetupRabbitMQContainer(&testing.T{})

	// Run tests
	code := m.Run()

	// Teardown
	if sharedContainer != nil {
		sharedContainer.Teardown(&testing.T{})
	}

	os.Exit(code)
}

func TestClient_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("New creates client and connects successfully", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, err := New(cfg)
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.True(t, client.IsConnected())

		err = client.Close()
		assert.NoError(t, err)
		assert.False(t, client.IsConnected())
	})

	t.Run("New with topology setup", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}
		cfg.Queues = []config.QueueConfig{
			{
				Name:        "test.queue",
				Durable:     true,
				Exchange:    "test.exchange",
				RoutingKeys: []string{"test.#"},
			},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		assert.True(t, client.IsConnected())
	})

	t.Run("GetChannel returns channel", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		ch, err := client.GetChannel()
		require.NoError(t, err)
		assert.NotNil(t, ch)
	})

	t.Run("NewContext creates context with timeout", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Timeout = 5 * time.Second

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		ctx, cancel := client.NewContext()
		defer cancel()

		deadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.True(t, time.Until(deadline) > 0)
		assert.True(t, time.Until(deadline) <= 5*time.Second)
	})

	t.Run("Close is idempotent", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, err := New(cfg)
		require.NoError(t, err)

		err = client.Close()
		assert.NoError(t, err)

		// Second close should not error
		err = client.Close()
		assert.NoError(t, err)
	})

	t.Run("GetChannel returns error when closed", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()

		client, err := New(cfg)
		require.NoError(t, err)

		_ = client.Close()

		_, err = client.GetChannel()
		assert.Error(t, err)
	})
}

func TestClient_DLQ(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("DLQ setup creates exchanges and queues", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.DLQEnabled = true
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "orders.exchange", Type: "topic", Durable: true},
		}
		cfg.Queues = []config.QueueConfig{
			{
				Name:        "orders.queue",
				Durable:     true,
				Exchange:    "orders.exchange",
				RoutingKeys: []string{"order.#"},
			},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		assert.True(t, client.IsConnected())
		// DLQ and DLX should be created automatically
	})

	t.Run("DLQ with custom config", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.DLQEnabled = true
		cfg.DLQConfig = config.DLQConfig{
			ExchangeName: "custom.dlx",
			QueuePrefix:  "failed.",
			ExchangeType: "topic",
			Durable:      true,
		}
		cfg.Queues = []config.QueueConfig{
			{Name: "test.custom.queue", Durable: true},
		}

		client, err := New(cfg)
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		assert.True(t, client.IsConnected())
	})
}

func TestClient_Errors(t *testing.T) {
	t.Run("New with invalid URI returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = "invalid://uri"
		cfg.Logger = logger.New()

		client, err := New(cfg)
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("New with unreachable host returns error", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = "amqp://guest:guest@localhost:9999/"
		cfg.Logger = logger.New()

		client, err := New(cfg)
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}
