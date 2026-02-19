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
	sharedContainer = rabbitTest.SetupRabbitMQContainer(&testing.T{})

	code := m.Run()

	if sharedContainer != nil {
		sharedContainer.Teardown(&testing.T{})
	}

	os.Exit(code)
}

// --- Test helpers ---

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

// captureHeaderHandler captures a specific header value for assertion in tests.
type captureHeaderHandler struct {
	key    string
	target *string
}

func (h *captureHeaderHandler) Execute(ctx *router.MessageContext) error {
	if h.key == "trace_id" {
		*h.target = ctx.GetTraceID()
	} else if v, ok := ctx.GetHeader(h.key).(string); ok {
		*h.target = v
	}
	return nil
}

// testEvent implements Event directly for testing
type testEvent struct {
	exchange  string
	eventType string
	message   string
	headers   map[string]any
}

func (e *testEvent) Type() string            { return e.eventType }
func (e *testEvent) Exchange() string        { return e.exchange }
func (e *testEvent) Headers() map[string]any { return e.headers }
func (e *testEvent) ToMap() map[string]any {
	return map[string]any{
		"type":    e.eventType,
		"message": e.message,
	}
}

// badEvent produces a ToMap that cannot be JSON-serialized (contains a channel).
type badEvent struct{}

func (e *badEvent) Type() string            { return "test.event" }
func (e *badEvent) Exchange() string        { return "test.exchange" }
func (e *badEvent) Headers() map[string]any { return nil }
func (e *badEvent) ToMap() map[string]any {
	return map[string]any{"bad": make(chan int)}
}

//nolint:unparam // exchange parameter kept for test flexibility
func newTestEvent(exchange, eventType, message string) Event {
	return &testEvent{
		exchange:  exchange,
		eventType: eventType,
		message:   message,
	}
}

func baseConfig() config.Config {
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

// --- NewEventBus ---

func TestNewEventBus(t *testing.T) {
	t.Run("invalid config returns error", func(t *testing.T) {
		_, err := NewEventBus(config.Config{URI: ""})
		assert.Error(t, err)
	})

	t.Run("nil logger uses default", func(t *testing.T) {
		cfg := config.DefaultConfig()
		cfg.URI = "amqp://guest:guest@localhost:5672/"
		cfg.Logger = nil

		// Will fail to connect but logger must not panic
		_, err := NewEventBus(cfg)
		assert.Error(t, err)
	})
}

func TestNewEventBus_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("connects successfully", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		assert.NotNil(t, eb)
	})
}

// --- RegisterHandler / StartConsume ---

func TestRegisterHandler_StartConsume_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("StartConsume without handlers returns error", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		err = eb.StartConsume("test.queue", 1)
		assert.Error(t, err)
	})

	t.Run("StartConsume with handler starts successfully", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		eb.RegisterHandler("test.event", &testEventHandler{})
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)
	})
}

// --- Close / IsConnected ---

func TestClose_IsConnected_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("IsConnected true before close, false after", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)

		assert.True(t, eb.IsConnected())

		_ = eb.Close()
		assert.False(t, eb.IsConnected())
	})
}

// --- GetCircuitBreakerMetrics / ResetCircuitBreaker ---

func TestCircuitBreaker_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("GetCircuitBreakerMetrics returns nil without consumer", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		assert.Nil(t, eb.GetCircuitBreakerMetrics())
	})

	t.Run("ResetCircuitBreaker returns false without consumer", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		assert.False(t, eb.ResetCircuitBreaker())
	})

	t.Run("GetCircuitBreakerMetrics returns metrics with active consumer", func(t *testing.T) {
		cfg := baseConfig()
		cfg.CircuitBreakerEnabled = true
		cfg.CircuitBreakerMaxFailures = 5
		cfg.CircuitBreakerResetTimeout = 10 * time.Second

		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		eb.RegisterHandler("test.event", &testEventHandler{})
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		metrics := eb.GetCircuitBreakerMetrics()
		require.NotNil(t, metrics)
		assert.Equal(t, "closed", metrics.State.String())
	})

	t.Run("ResetCircuitBreaker returns true with active consumer", func(t *testing.T) {
		cfg := baseConfig()
		cfg.CircuitBreakerEnabled = true

		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		eb.RegisterHandler("test.event", &testEventHandler{})
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		assert.True(t, eb.ResetCircuitBreaker())
	})
}

// --- RegisterDLQHandler / StartConsumeDLQ ---

func TestRegisterDLQHandler_StartConsumeDLQ_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	dlqConfig := func() config.Config {
		cfg := baseConfig()
		cfg.Queues = append(cfg.Queues, config.QueueConfig{
			Name:        "dlq.test.queue",
			Durable:     true,
			Exchange:    "test.exchange",
			RoutingKeys: []string{"dlq.#"},
		})
		return cfg
	}

	t.Run("StartConsumeDLQ without handlers returns error", func(t *testing.T) {
		eb, err := NewEventBus(dlqConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		err = eb.StartConsumeDLQ("dlq.test.queue", 1)
		assert.Error(t, err)
	})

	t.Run("RegisterDLQHandler and StartConsumeDLQ starts successfully", func(t *testing.T) {
		eb, err := NewEventBus(dlqConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		eb.RegisterDLQHandler("test.event", &testEventHandler{})
		go func() { _ = eb.StartConsumeDLQ("dlq.test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)
	})
}

// --- RequeueFromDLQ ---

func TestRequeueFromDLQ_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("requeues message successfully", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

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

	t.Run("missing exchange returns error", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

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
}

// --- RequeueAllFromDLQ ---

func TestRequeueAllFromDLQ_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("returns error on closed client", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		_ = eb.Close()

		count, err := eb.RequeueAllFromDLQ(context.Background(), "dlq.test.queue", true, 10)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
	})
}
