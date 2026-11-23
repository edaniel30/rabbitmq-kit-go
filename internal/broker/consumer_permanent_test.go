package broker

import (
	"context"
	stderrors "errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/internal/circuitbreaker"
	"github.com/edaniel30/rabbitmq-kit-go/internal/logger"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingHandler counts executions and fails with whatever fail returns.
type countingHandler struct {
	calls atomic.Int32
	fail  func(body string) error
}

func (h *countingHandler) Execute(ctx *router.MessageContext) error {
	h.calls.Add(1)
	var data map[string]any
	_ = ctx.BindJSON(&data)
	msg, _ := data["message"].(string)
	return h.fail(msg)
}

var errNotFound = stderrors.New("entity not found")

func TestConsumer_PermanentErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// setup declares queueName (with its DLQ when dlq is true) bound to
	// "perm.exchange" with routing key queueName.
	setup := func(t *testing.T, queueName string, dlq bool, opts ...config.Option) (*Client, *Publisher, *router.Router) {
		t.Helper()
		cfg := config.DefaultConfig()
		cfg.URI = sharedContainer.URI
		cfg.Logger = logger.New()
		cfg.DLQEnabled = dlq
		cfg.Exchanges = []config.ExchangeConfig{{Name: "perm.exchange", Type: "direct", Durable: true}}
		cfg.Queues = []config.QueueConfig{{Name: queueName, Durable: true, Exchange: "perm.exchange", RoutingKeys: []string{queueName}}}
		for _, opt := range opts {
			opt(&cfg)
		}
		client, err := New(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		publisher := NewPublisher(client)
		t.Cleanup(publisher.Close)
		return client, publisher, router.NewRouter()
	}

	publish := func(t *testing.T, p *Publisher, queueName, eventType, message string) {
		t.Helper()
		require.NoError(t, p.Publish(context.Background(), "perm.exchange", queueName, amqp.Publishing{
			ContentType: "application/json",
			Type:        eventType,
			Body:        []byte(`{"type":"` + eventType + `","message":"` + message + `"}`),
		}))
	}

	// queueDepth returns how many messages wait in a queue.
	queueDepth := func(t *testing.T, queue string) int {
		t.Helper()
		conn, err := amqp.Dial(sharedContainer.URI)
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		require.NoError(t, err)
		defer func() { _ = ch.Close() }()
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		require.NoError(t, err)
		return q.Messages
	}

	t.Run("a wrapped permanent error is not retried and goes to the DLQ", func(t *testing.T) {
		client, publisher, r := setup(t, "perm.wrapped", true)
		h := &countingHandler{fail: func(string) error { return errors.NewPermanentError(errNotFound) }}
		r.Handle("perm.wrapped", h)

		go func() { _ = NewConsumer(client, publisher, r).Consume("perm.wrapped", 1) }()
		time.Sleep(100 * time.Millisecond)
		publish(t, publisher, "perm.wrapped", "perm.wrapped", "x")

		assert.Eventually(t, func() bool { return queueDepth(t, "dlq.perm.wrapped") == 1 }, 3*time.Second, 50*time.Millisecond)
		assert.Equal(t, int32(1), h.calls.Load(), "executed once: no retries")
	})

	t.Run("the classifier marks plain errors permanent", func(t *testing.T) {
		client, publisher, r := setup(t, "perm.classified", true,
			config.WithPermanentErrors(func(err error) bool { return stderrors.Is(err, errNotFound) }))
		h := &countingHandler{fail: func(string) error { return errNotFound }}
		r.Handle("perm.classified", h)

		go func() { _ = NewConsumer(client, publisher, r).Consume("perm.classified", 1) }()
		time.Sleep(100 * time.Millisecond)
		publish(t, publisher, "perm.classified", "perm.classified", "x")

		assert.Eventually(t, func() bool { return queueDepth(t, "dlq.perm.classified") == 1 }, 3*time.Second, 50*time.Millisecond)
		assert.Equal(t, int32(1), h.calls.Load())
	})

	t.Run("an expected retry is retried even if the classifier matches", func(t *testing.T) {
		client, publisher, r := setup(t, "perm.expected", false,
			config.WithMaxRetries(2),
			config.WithPermanentErrors(func(err error) bool { return stderrors.Is(err, errNotFound) }))
		h := &countingHandler{fail: func(string) error { return errors.NewExpectedRetryError(errNotFound) }}
		r.Handle("perm.expected", h)

		go func() { _ = NewConsumer(client, publisher, r).Consume("perm.expected", 1) }()
		time.Sleep(100 * time.Millisecond)
		publish(t, publisher, "perm.expected", "perm.expected", "x")

		assert.Eventually(t, func() bool { return h.calls.Load() == 3 }, 5*time.Second, 50*time.Millisecond, "first try + 2 retries")
	})

	t.Run("permanent errors do not open the circuit breaker", func(t *testing.T) {
		client, publisher, r := setup(t, "perm.breaker", false,
			config.WithCircuitBreaker(true),
			config.WithCircuitBreakerMaxFailures(2),
			config.WithCircuitBreakerResetTimeout(time.Minute))
		h := &countingHandler{fail: func(msg string) error {
			if msg == "bad" {
				return errors.NewPermanentError(errNotFound)
			}
			return nil
		}}
		r.Handle("perm.breaker", h)

		consumer := NewConsumer(client, publisher, r)
		go func() { _ = consumer.Consume("perm.breaker", 1) }()
		time.Sleep(100 * time.Millisecond)
		for range 5 {
			publish(t, publisher, "perm.breaker", "perm.breaker", "bad")
		}
		publish(t, publisher, "perm.breaker", "perm.breaker", "good")

		// All six reach the handler: the breaker stayed closed
		assert.Eventually(t, func() bool { return h.calls.Load() == 6 }, 3*time.Second, 50*time.Millisecond)
		assert.Equal(t, circuitbreaker.StateClosed, consumer.GetCircuitBreakerMetrics().State)
	})
}
