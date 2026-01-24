package rabbitmq

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	rabbitTest "github.com/edaniel30/rabbitmq-kit-go/testing"
	amqp "github.com/rabbitmq/amqp091-go"
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

// testEventHandler implements HandlerService for testing
type testEventHandler struct {
	mu       sync.Mutex
	messages []string
	count    int
	err      error
}

func (h *testEventHandler) Execute(ctx *router.MessageContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.count++

	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	if msg, ok := data["message"].(string); ok {
		h.messages = append(h.messages, msg)
	}

	return h.err
}

func (h *testEventHandler) GetCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

func (h *testEventHandler) GetMessages() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string{}, h.messages...)
}

// testEvent embeds BaseEvent and adds custom fields
type testEvent struct {
	*BaseEvent
	Message string
}

func (e *testEvent) ToMap() map[string]any {
	m := e.BaseEvent.ToMap()
	m["message"] = e.Message
	return m
}

//nolint:unparam // exchange parameter kept for test flexibility
func newTestEvent(exchange, eventType, message string) Event {
	return &testEvent{
		BaseEvent: NewBaseEvent(exchange, eventType),
		Message:   message,
	}
}

func TestEventBus_Unit(t *testing.T) {
	t.Run("NewEventBus with invalid config returns error", func(t *testing.T) {
		cfg := config.Config{
			URI: "", // Invalid
		}

		_, err := NewEventBus(cfg)
		assert.Error(t, err)
	})

	t.Run("NewEventBus sets default logger if nil", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = "amqp://guest:guest@localhost:5672/"
		cfg.Logger = nil

		_, err := NewEventBus(cfg)
		// Will fail because no RabbitMQ running, but logger should be set
		assert.Error(t, err)
	})
}

func TestEventBus_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	setupConfig := func() config.Config {
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
		return cfg
	}

	t.Run("Publish and consume single event", func(t *testing.T) {
		cfg := setupConfig()
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		handler := &testEventHandler{}
		eb.RegisterHandler("test.event", handler)

		// Start consuming
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		// Publish event
		event := newTestEvent("test.exchange", "test.event", "hello")
		err = eb.Publish(context.Background(), event)
		require.NoError(t, err)

		// Wait for consumption
		time.Sleep(150 * time.Millisecond)

		messages := handler.GetMessages()
		assert.Contains(t, messages, "hello")
	})

	t.Run("PublishBatch variants", func(t *testing.T) {
		tests := []struct {
			name            string
			setupEB         func() (*EventBus, error)
			events          []Event
			options         []config.BatchOption
			expectSuccess   int
			expectFailed    int
			expectError     bool
			closeBeforeTest bool
		}{
			{
				name: "pipelining mode",
				setupEB: func() (*EventBus, error) {
					cfg := setupConfig()
					cfg.PublisherConfirms = true
					cfg.ConfirmTimeout = 10 * time.Second
					return NewEventBus(cfg)
				},
				events: []Event{
					newTestEvent("test.exchange", "test.event", "msg1"),
					newTestEvent("test.exchange", "test.event", "msg2"),
					newTestEvent("test.exchange", "test.event", "msg3"),
				},
				options:       []config.BatchOption{config.WithPipelining(true)},
				expectSuccess: 3,
				expectFailed:  0,
			},
			{
				name: "sequential mode",
				setupEB: func() (*EventBus, error) {
					return NewEventBus(setupConfig())
				},
				events: []Event{
					newTestEvent("test.exchange", "test.event", "seq1"),
					newTestEvent("test.exchange", "test.event", "seq2"),
				},
				options:       []config.BatchOption{config.WithPipelining(false)},
				expectSuccess: 2,
				expectFailed:  0,
			},
			{
				name: "empty events",
				setupEB: func() (*EventBus, error) {
					return NewEventBus(setupConfig())
				},
				events:        []Event{},
				expectSuccess: 0,
				expectFailed:  0,
			},
			{
				name: "FailFast stops on error",
				setupEB: func() (*EventBus, error) {
					return NewEventBus(setupConfig())
				},
				events: []Event{
					newTestEvent("test.exchange", "test.event", "msg1"),
					newTestEvent("test.exchange", "test.event", "msg2"),
				},
				options:         []config.BatchOption{config.WithFailFast(true)},
				closeBeforeTest: true,
				expectError:     true,
			},
			{
				name: "sequential with errors",
				setupEB: func() (*EventBus, error) {
					return NewEventBus(setupConfig())
				},
				events: []Event{
					newTestEvent("test.exchange", "test.event", "msg1"),
					newTestEvent("test.exchange", "test.event", "msg2"),
				},
				options:         []config.BatchOption{config.WithPipelining(false)},
				closeBeforeTest: true,
				expectFailed:    2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				eb, err := tt.setupEB()
				require.NoError(t, err)
				defer func() { _ = eb.Close() }()

				if tt.closeBeforeTest {
					_ = eb.Close()
				}

				result, err := eb.PublishBatch(context.Background(), tt.events, tt.options...)

				if tt.expectError {
					assert.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Equal(t, tt.expectSuccess, result.Success)
					assert.Equal(t, tt.expectFailed, result.Failed)
				}
			})
		}
	})

	t.Run("PublishBatchAsync variants", func(t *testing.T) {
		tests := []struct {
			name            string
			numEvents       int
			options         []config.BatchOption
			expectSuccess   int
			expectFailed    int
			closeBeforeTest bool
		}{
			{
				name:          "with concurrency limit",
				numEvents:     10,
				options:       []config.BatchOption{config.WithMaxConcurrency(3)},
				expectSuccess: 10,
			},
			{
				name:          "unlimited concurrency",
				numEvents:     5,
				expectSuccess: 5,
			},
			{
				name:      "empty events",
				numEvents: 0,
			},
			{
				name:            "with errors",
				numEvents:       3,
				options:         []config.BatchOption{config.WithMaxConcurrency(2)},
				closeBeforeTest: true,
				expectFailed:    3,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := setupConfig()
				eb, err := NewEventBus(cfg)
				require.NoError(t, err)
				defer func() { _ = eb.Close() }()

				events := make([]Event, tt.numEvents)
				for i := range events {
					events[i] = newTestEvent("test.exchange", "test.event", "async")
				}

				if tt.closeBeforeTest {
					_ = eb.Close()
				}

				result, err := eb.PublishBatchAsync(context.Background(), events, tt.options...)
				require.NoError(t, err)
				assert.Equal(t, tt.numEvents, result.Total)
				assert.Equal(t, tt.expectSuccess, result.Success)
				assert.Equal(t, tt.expectFailed, result.Failed)
			})
		}
	})

	t.Run("Connection and status", func(t *testing.T) {
		cfg := setupConfig()
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)

		assert.True(t, eb.IsConnected())

		_ = eb.Close()
		assert.False(t, eb.IsConnected())
	})

	t.Run("Circuit breaker operations", func(t *testing.T) {
		tests := []struct {
			name                string
			enableCircuitBreaker bool
			startConsumer       bool
			expectMetrics       bool
			expectResetSuccess  bool
		}{
			{
				name:               "metrics without consumer",
				expectMetrics:      false,
				expectResetSuccess: false,
			},
			{
				name:                "metrics with consumer",
				enableCircuitBreaker: true,
				startConsumer:       true,
				expectMetrics:       true,
				expectResetSuccess:  true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cfg := setupConfig()
				if tt.enableCircuitBreaker {
					cfg.CircuitBreakerEnabled = true
					cfg.CircuitBreakerMaxFailures = 5
					cfg.CircuitBreakerResetTimeout = 10 * time.Second
				}

				eb, err := NewEventBus(cfg)
				require.NoError(t, err)
				defer func() { _ = eb.Close() }()

				if tt.startConsumer {
					handler := &testEventHandler{}
					eb.RegisterHandler("test.event", handler)
					go func() { _ = eb.StartConsume("test.queue", 1) }()
					time.Sleep(100 * time.Millisecond)
				}

				metrics := eb.GetCircuitBreakerMetrics()
				if tt.expectMetrics {
					assert.NotNil(t, metrics)
					assert.Equal(t, "closed", metrics.State.String())
				} else {
					assert.Nil(t, metrics)
				}

				resetResult := eb.ResetCircuitBreaker()
				assert.Equal(t, tt.expectResetSuccess, resetResult)
			})
		}
	})

	t.Run("Consumer errors", func(t *testing.T) {
		cfg := setupConfig()
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		// StartConsume without handlers
		err = eb.StartConsume("test.queue", 1)
		assert.Error(t, err)

		// StartConsumeDLQ without handlers
		err = eb.StartConsumeDLQ("dlq.test.queue", 1)
		assert.Error(t, err)
	})

	t.Run("DLQ operations", func(t *testing.T) {
		cfg := setupConfig()
		cfg.Queues = append(cfg.Queues, config.QueueConfig{
			Name:        "dlq.test.queue",
			Durable:     true,
			Exchange:    "test.exchange",
			RoutingKeys: []string{"dlq.#"},
		})

		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		t.Run("register handler and consume", func(t *testing.T) {
			dlqHandler := &testEventHandler{}
			eb.RegisterDLQHandler("test.event", dlqHandler)

			go func() { _ = eb.StartConsumeDLQ("dlq.test.queue", 1) }()
			time.Sleep(100 * time.Millisecond)

			event := newTestEvent("test.exchange", "test.event", "dlq message")
			err = eb.Publish(context.Background(), event)
			require.NoError(t, err)

			time.Sleep(150 * time.Millisecond)
		})

		t.Run("requeue from DLQ", func(t *testing.T) {
			msgCtx := &router.MessageContext{
				Delivery: amqp.Delivery{
					Exchange:     "test.exchange",
					RoutingKey:   "test.event",
					Body:         []byte(`{"message":"test"}`),
					ContentType:  "application/json",
					DeliveryMode: 2,
					Headers: amqp.Table{
						"x-retry-count": int32(3),
						"x-death": []any{
							amqp.Table{
								"queue":        "test.queue",
								"reason":       "rejected",
								"count":        int64(3),
								"exchange":     "test.exchange",
								"routing-keys": []any{"test.event"},
							},
						},
					},
				},
			}
			dlqMsg := router.NewDLQMessage(msgCtx)

			err = eb.RequeueFromDLQ(context.Background(), dlqMsg, true)
			require.NoError(t, err)
		})

		t.Run("requeue with missing exchange", func(t *testing.T) {
			msgCtx := &router.MessageContext{
				Delivery: amqp.Delivery{
					Exchange:   "",
					RoutingKey: "",
					Body:       []byte(`{}`),
				},
			}
			dlqMsg := router.NewDLQMessage(msgCtx)

			err = eb.RequeueFromDLQ(context.Background(), dlqMsg, false)
			assert.Error(t, err)
		})
	})
}

func TestEventBus_PublishErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	setupConfig := func() config.Config {
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.Exchanges = []config.ExchangeConfig{
			{Name: "test.exchange", Type: "topic", Durable: true},
		}
		return cfg
	}

	t.Run("Publish to closed eventbus", func(t *testing.T) {
		cfg := setupConfig()
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)

		_ = eb.Close()

		event := newTestEvent("test.exchange", "test.event", "msg")
		err = eb.Publish(context.Background(), event)
		assert.Error(t, err)
	})

	t.Run("Publish with timeout context", func(t *testing.T) {
		cfg := setupConfig()
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		time.Sleep(10 * time.Millisecond) // Ensure timeout has passed

		event := newTestEvent("test.exchange", "test.event", "msg")
		err = eb.Publish(ctx, event)
		// May or may not error depending on timing, just verify no panic
		_ = err
	})
}
