package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, 5*time.Second, cfg.ReconnectDelay)
	assert.Equal(t, 10*time.Second, cfg.Timeout)
	assert.Equal(t, 10, cfg.PrefetchCount)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.False(t, cfg.PublisherConfirms)
	assert.NotNil(t, cfg.Logger)
	assert.False(t, cfg.CircuitBreakerEnabled)
	assert.Equal(t, 5, cfg.CircuitBreakerMaxFailures)
	assert.Equal(t, 60*time.Second, cfg.CircuitBreakerResetTimeout)
	assert.Equal(t, 3, cfg.CircuitBreakerHalfOpenRequests)
	assert.False(t, cfg.DLQEnabled)
	assert.NotNil(t, cfg.DLQConfig)
	assert.Empty(t, cfg.Exchanges)
	assert.Empty(t, cfg.Queues)
}

func TestConfigOptions(t *testing.T) {
	tests := []struct {
		name     string
		option   Option
		validate func(t *testing.T, cfg Config)
	}{
		{
			name:   "WithURI",
			option: WithURI("amqp://test:5672"),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, "amqp://test:5672", cfg.URI)
			},
		},
		{
			name:   "WithReconnectDelay",
			option: WithReconnectDelay(2 * time.Second),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 2*time.Second, cfg.ReconnectDelay)
			},
		},
		{
			name:   "WithTimeout",
			option: WithTimeout(30 * time.Second),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 30*time.Second, cfg.Timeout)
			},
		},
		{
			name:   "WithPrefetchCount",
			option: WithPrefetchCount(20),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 20, cfg.PrefetchCount)
			},
		},
		{
			name:   "WithMaxRetries",
			option: WithMaxRetries(5),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 5, cfg.MaxRetries)
			},
		},
		{
			name:   "WithPublisherConfirms",
			option: WithPublisherConfirms(true),
			validate: func(t *testing.T, cfg Config) {
				assert.True(t, cfg.PublisherConfirms)
			},
		},
		{
			name:   "WithConfirmTimeout",
			option: WithConfirmTimeout(15 * time.Second),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 15*time.Second, cfg.ConfirmTimeout)
			},
		},
		{
			name:   "WithLogger",
			option: WithLogger(DefaultConfig().Logger),
			validate: func(t *testing.T, cfg Config) {
				assert.NotNil(t, cfg.Logger)
			},
		},
		{
			name:   "WithCircuitBreaker",
			option: WithCircuitBreaker(true),
			validate: func(t *testing.T, cfg Config) {
				assert.True(t, cfg.CircuitBreakerEnabled)
			},
		},
		{
			name:   "WithCircuitBreakerMaxFailures",
			option: WithCircuitBreakerMaxFailures(10),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 10, cfg.CircuitBreakerMaxFailures)
			},
		},
		{
			name:   "WithCircuitBreakerResetTimeout",
			option: WithCircuitBreakerResetTimeout(30 * time.Second),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 30*time.Second, cfg.CircuitBreakerResetTimeout)
			},
		},
		{
			name:   "WithCircuitBreakerHalfOpenRequests",
			option: WithCircuitBreakerHalfOpenRequests(5),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, 5, cfg.CircuitBreakerHalfOpenRequests)
			},
		},
		{
			name:   "WithDLQ",
			option: WithDLQ(true),
			validate: func(t *testing.T, cfg Config) {
				assert.True(t, cfg.DLQEnabled)
			},
		},
		{
			name:   "WithCustomDLQ",
			option: WithCustomDLQ(DLQConfig{ExchangeName: "custom.dlx"}),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, "custom.dlx", cfg.DLQConfig.ExchangeName)
			},
		},
		{
			name:   "WithDLQExchange",
			option: WithDLQExchange("my.dlx"),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, "my.dlx", cfg.DLQConfig.ExchangeName)
			},
		},
		{
			name:   "WithDLQPrefix",
			option: WithDLQPrefix("failed."),
			validate: func(t *testing.T, cfg Config) {
				assert.Equal(t, "failed.", cfg.DLQConfig.QueuePrefix)
			},
		},
		{
			name:   "WithExchanges",
			option: WithExchanges([]ExchangeConfig{{Name: "test.exchange", Type: "direct"}}),
			validate: func(t *testing.T, cfg Config) {
				require.Len(t, cfg.Exchanges, 1)
				assert.Equal(t, "test.exchange", cfg.Exchanges[0].Name)
				assert.Equal(t, "direct", cfg.Exchanges[0].Type)
			},
		},
		{
			name: "WithQueues",
			option: WithQueues([]QueueConfig{
				{Name: "test.queue", Exchange: "test.exchange"},
			}),
			validate: func(t *testing.T, cfg Config) {
				require.Len(t, cfg.Queues, 1)
				assert.Equal(t, "test.queue", cfg.Queues[0].Name)
				assert.Equal(t, "test.exchange", cfg.Queues[0].Exchange)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.option(&cfg)
			tt.validate(t, cfg)
		})
	}
}

func TestConfigValidation(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := DefaultConfig()
		WithURI("amqp://localhost:5672")(&cfg)
		WithPrefetchCount(50)(&cfg)
		WithMaxRetries(5)(&cfg)
		WithPublisherConfirms(true)(&cfg)
		WithCircuitBreaker(true)(&cfg)

		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("validation errors", func(t *testing.T) {
		tests := []struct {
			name     string
			setup    func(cfg *Config)
			errorMsg string
		}{
			{
				name:     "missing URI",
				setup:    func(cfg *Config) { cfg.URI = "" },
				errorMsg: "URI",
			},
			{
				name:     "zero ReconnectDelay",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.ReconnectDelay = 0 },
				errorMsg: "ReconnectDelay",
			},
			{
				name:     "negative Timeout",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.Timeout = -1 * time.Second },
				errorMsg: "Timeout",
			},
			{
				name:     "negative PrefetchCount",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.PrefetchCount = -1 },
				errorMsg: "PrefetchCount",
			},
			{
				name:     "negative MaxRetries",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.MaxRetries = -1 },
				errorMsg: "MaxRetries",
			},
			{
				name:     "exchange without name",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.Exchanges = []ExchangeConfig{{Type: "direct"}} },
				errorMsg: "Exchanges[0].Name",
			},
			{
				name:     "exchange without type",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.Exchanges = []ExchangeConfig{{Name: "test"}} },
				errorMsg: "Exchanges[0].Type",
			},
			{
				name:     "queue without name",
				setup:    func(cfg *Config) { cfg.URI = "amqp://test"; cfg.Queues = []QueueConfig{{Exchange: "test"}} },
				errorMsg: "Queues[0].Name",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := DefaultConfig()
				tt.setup(&cfg)

				err := cfg.Validate()
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			})
		}
	})
}

func TestBatchConfig(t *testing.T) {
	t.Run("DefaultBatchConfig", func(t *testing.T) {
		cfg := DefaultBatchConfig()
		assert.True(t, cfg.UsePipelining)
		assert.False(t, cfg.FailFast)
		assert.Equal(t, 0, cfg.MaxConcurrency)
	})

	t.Run("WithPipelining", func(t *testing.T) {
		cfg := DefaultBatchConfig()
		WithPipelining(false)(&cfg)
		assert.False(t, cfg.UsePipelining)
	})

	t.Run("WithFailFast", func(t *testing.T) {
		cfg := DefaultBatchConfig()
		WithFailFast(true)(&cfg)
		assert.True(t, cfg.FailFast)
	})

	t.Run("WithMaxConcurrency", func(t *testing.T) {
		cfg := DefaultBatchConfig()
		WithMaxConcurrency(50)(&cfg)
		assert.Equal(t, 50, cfg.MaxConcurrency)
	})
}

func TestDLQConfig(t *testing.T) {
	t.Run("DefaultDLQConfig", func(t *testing.T) {
		cfg := DefaultDLQConfig()
		assert.Equal(t, "dlx.exchange", cfg.ExchangeName)
		assert.Equal(t, "dlq.", cfg.QueuePrefix)
		assert.Equal(t, "direct", cfg.ExchangeType)
		assert.True(t, cfg.Durable)
		assert.False(t, cfg.AutoDelete)
	})

	t.Run("GetDLQName", func(t *testing.T) {
		cfg := DefaultDLQConfig()
		assert.Equal(t, "dlq.payments", cfg.GetDLQName("payments"))
	})

	t.Run("GetDLXRoutingKey", func(t *testing.T) {
		cfg := DefaultDLQConfig()
		assert.Equal(t, "orders.queue", cfg.GetDLXRoutingKey("orders.queue"))
	})
}

func TestQueueConfig(t *testing.T) {
	t.Run("WithDLX", func(t *testing.T) {
		q := &QueueConfig{Name: "test"}
		q.WithDLX("dlx.exchange", "dlx.key")

		assert.NotNil(t, q.Args)
		assert.Equal(t, "dlx.exchange", q.Args["x-dead-letter-exchange"])
		assert.Equal(t, "dlx.key", q.Args["x-dead-letter-routing-key"])

		// Test idempotency
		q.WithDLX("other.exchange", "other.key")
		assert.Equal(t, "dlx.exchange", q.Args["x-dead-letter-exchange"], "should not override")
	})

	t.Run("CreateDLQQueue", func(t *testing.T) {
		main := QueueConfig{Name: "payments", Exchange: "payments.ex"}
		dlqCfg := DefaultDLQConfig()

		dlq := CreateDLQQueue(main, dlqCfg)

		assert.Equal(t, "dlq.payments", dlq.Name)
		assert.Equal(t, "dlx.exchange", dlq.Exchange)
		assert.True(t, dlq.Durable)
		assert.False(t, dlq.AutoDelete)
		require.Len(t, dlq.RoutingKeys, 1)
		assert.Equal(t, "payments", dlq.RoutingKeys[0])
	})
}
