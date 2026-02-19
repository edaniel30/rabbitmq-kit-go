package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Publish ---

func TestPublish_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("publishes and consumes single event", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		handler := &testEventHandler{}
		eb.RegisterHandler("test.event", handler)
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		err = eb.Publish(context.Background(), newTestEvent("test.exchange", "test.event", "hello"))
		require.NoError(t, err)

		time.Sleep(150 * time.Millisecond)
		assert.Contains(t, handler.GetMessages(), "hello")
	})

	t.Run("auto-generates trace_id header", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		var capturedTraceID string
		eb.RegisterHandler("test.event", &captureHeaderHandler{key: "trace_id", target: &capturedTraceID})
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		err = eb.Publish(context.Background(), newTestEvent("test.exchange", "test.event", "trace test"))
		require.NoError(t, err)

		time.Sleep(150 * time.Millisecond)
		assert.NotEmpty(t, capturedTraceID)
	})

	t.Run("preserves existing trace_id header", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		const expectedTraceID = "my-custom-trace-id"
		var capturedTraceID string
		eb.RegisterHandler("test.event", &captureHeaderHandler{key: "trace_id", target: &capturedTraceID})
		go func() { _ = eb.StartConsume("test.queue", 1) }()
		time.Sleep(100 * time.Millisecond)

		event := &testEvent{
			exchange:  "test.exchange",
			eventType: "test.event",
			message:   "preserve trace",
			headers:   map[string]any{"trace_id": expectedTraceID},
		}
		err = eb.Publish(context.Background(), event)
		require.NoError(t, err)

		time.Sleep(150 * time.Millisecond)
		assert.Equal(t, expectedTraceID, capturedTraceID)
	})

	t.Run("returns error on closed eventbus", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		_ = eb.Close()

		err = eb.Publish(context.Background(), newTestEvent("test.exchange", "test.event", "msg"))
		assert.Error(t, err)
	})
}

// --- PublishBatch ---

func TestPublishBatch_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("pipelining mode publishes all messages", func(t *testing.T) {
		cfg := baseConfig()
		cfg.PublisherConfirms = true
		cfg.ConfirmTimeout = 10 * time.Second

		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "msg1"),
			newTestEvent("test.exchange", "test.event", "msg2"),
			newTestEvent("test.exchange", "test.event", "msg3"),
		}
		result, err := eb.PublishBatch(context.Background(), events, config.WithPipelining(true))
		require.NoError(t, err)
		assert.Equal(t, 3, result.Success)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("sequential mode publishes all messages", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "seq1"),
			newTestEvent("test.exchange", "test.event", "seq2"),
		}
		result, err := eb.PublishBatch(context.Background(), events, config.WithPipelining(false))
		require.NoError(t, err)
		assert.Equal(t, 2, result.Success)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("empty batch returns zero result", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		result, err := eb.PublishBatch(context.Background(), []Event{})
		require.NoError(t, err)
		assert.Equal(t, 0, result.Total)
		assert.Equal(t, 0, result.Success)
		assert.Equal(t, 0, result.Failed)
	})

	t.Run("FailFast stops on first error", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		_ = eb.Close()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "msg1"),
			newTestEvent("test.exchange", "test.event", "msg2"),
		}
		_, err = eb.PublishBatch(context.Background(), events, config.WithFailFast(true))
		assert.Error(t, err)
	})

	t.Run("sequential mode accumulates errors without stopping", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		_ = eb.Close()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "msg1"),
			newTestEvent("test.exchange", "test.event", "msg2"),
		}
		result, _ := eb.PublishBatch(context.Background(), events, config.WithPipelining(false))
		if result != nil {
			assert.Equal(t, 2, result.Failed)
		}
	})

	t.Run("serialization error is accumulated as failed", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		result, err := eb.PublishBatch(context.Background(), []Event{&badEvent{}}, config.WithPipelining(true))
		require.NoError(t, err)
		assert.Equal(t, 1, result.Failed)
	})

	t.Run("FailFast returns error on serialization failure", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := []Event{&badEvent{}, newTestEvent("test.exchange", "test.event", "ok")}
		_, err = eb.PublishBatch(context.Background(), events,
			config.WithPipelining(true),
			config.WithFailFast(true),
		)
		assert.Error(t, err)
	})

	t.Run("with MaxConcurrency publishes all messages", func(t *testing.T) {
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := make([]Event, 10)
		for i := range events {
			events[i] = newTestEvent("test.exchange", "test.event", "async")
		}

		result, err := eb.PublishBatch(context.Background(), events, config.WithMaxConcurrency(3))
		require.NoError(t, err)
		assert.Equal(t, 10, result.Total)
		assert.Equal(t, 10, result.Success)
	})
}
