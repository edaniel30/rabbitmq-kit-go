package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	kiterrors "github.com/edaniel30/rabbitmq-kit-go/errors"
	"github.com/edaniel30/rabbitmq-kit-go/router"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setupTelemetry(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
	return recorder
}

func TestCreateDefaultPublishing_TraceID(t *testing.T) {
	t.Run("without telemetry: new UUID, no traceparent", func(t *testing.T) {
		msg, err := createDefaultPublishing(context.Background(), newTestEvent("ex", "test.event", "m"))
		require.NoError(t, err)

		_, err = uuid.Parse(msg.Headers["trace_id"].(string))
		assert.NoError(t, err)
		assert.NotContains(t, msg.Headers, "traceparent")
	})

	t.Run("with a span: trace_id is the OpenTelemetry trace ID and traceparent is set", func(t *testing.T) {
		setupTelemetry(t)
		ctx, span := otel.Tracer("test").Start(context.Background(), "request")
		defer span.End()

		msg, err := createDefaultPublishing(ctx, newTestEvent("ex", "test.event", "m"))
		require.NoError(t, err)

		assert.Equal(t, span.SpanContext().TraceID().String(), msg.Headers["trace_id"])
		assert.NotEmpty(t, msg.Headers["traceparent"])
	})

	t.Run("keeps the event's trace_id", func(t *testing.T) {
		setupTelemetry(t)
		ctx, span := otel.Tracer("test").Start(context.Background(), "request")
		defer span.End()

		event := &testEvent{exchange: "ex", eventType: "test.event", headers: map[string]any{"trace_id": "custom"}}
		msg, err := createDefaultPublishing(ctx, event)
		require.NoError(t, err)
		assert.Equal(t, "custom", msg.Headers["trace_id"])
	})

	t.Run("replaces an empty trace_id", func(t *testing.T) {
		event := &testEvent{exchange: "ex", eventType: "test.event", headers: map[string]any{"trace_id": ""}}
		msg, err := createDefaultPublishing(context.Background(), event)
		require.NoError(t, err)
		assert.NotEmpty(t, msg.Headers["trace_id"])
	})
}

// contextCaptureHandler records the span context of the message whose body has the given
// message (the test queue may still hold messages from other tests).
type contextCaptureHandler struct {
	message string
	mu      sync.Mutex
	once    sync.Once
	sc      trace.SpanContext
	done    chan struct{}
}

func (h *contextCaptureHandler) Execute(msg *router.MessageContext) error {
	var body struct {
		Message string `json:"message"`
	}
	if err := msg.BindJSON(&body); err != nil {
		return err
	}
	if body.Message != h.message {
		return nil
	}
	h.once.Do(func() {
		h.mu.Lock()
		h.sc = trace.SpanContextFromContext(msg.Context())
		h.mu.Unlock()
		close(h.done)
	})
	return nil
}

func TestTracing_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	recorder := setupTelemetry(t)

	eb, err := NewEventBus(baseConfig())
	require.NoError(t, err)
	defer func() { _ = eb.Close() }()

	handler := &contextCaptureHandler{message: "traced", done: make(chan struct{})}
	eb.RegisterHandler("test.event", handler)
	go func() { _ = eb.StartConsume("test.queue", 1) }()
	time.Sleep(100 * time.Millisecond)

	reqCtx, reqSpan := otel.Tracer("test").Start(context.Background(), "POST /things")
	require.NoError(t, eb.Publish(reqCtx, newTestEvent("test.exchange", "test.event", "traced")))
	reqSpan.End()

	select {
	case <-handler.done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not called")
	}

	handler.mu.Lock()
	handlerSC := handler.sc
	handler.mu.Unlock()

	// The handler's context belongs to the publishing request's trace
	assert.Equal(t, reqSpan.SpanContext().TraceID(), handlerSC.TraceID())

	assert.Eventually(t, func() bool {
		var pub, cons bool
		for _, s := range recorder.Ended() {
			if s.SpanContext().TraceID() != reqSpan.SpanContext().TraceID() {
				continue
			}
			switch s.Name() {
			case "publish test.event":
				pub = s.SpanKind() == trace.SpanKindProducer
			case "process test.event":
				cons = s.SpanKind() == trace.SpanKindConsumer && s.SpanContext().SpanID() == handlerSC.SpanID()
			}
		}
		return pub && cons
	}, 2*time.Second, 50*time.Millisecond)
}

// publishSpans returns the ended PRODUCER spans of the recorder.
func publishSpans(recorder *tracetest.SpanRecorder) []sdktrace.ReadOnlySpan {
	var spans []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.SpanKind() == trace.SpanKindProducer {
			spans = append(spans, s)
		}
	}
	return spans
}

func TestPublishBatchTracing_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Run("one span per event, error status on serialization failure", func(t *testing.T) {
		recorder := setupTelemetry(t)
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "a"),
			&badEvent{},
			newTestEvent("test.exchange", "test.event", "b"),
		}
		result, err := eb.PublishBatch(context.Background(), events, config.WithPipelining(true))
		require.NoError(t, err)
		assert.Equal(t, 1, result.Failed)

		assert.Len(t, recorder.Started(), 3)
		spans := publishSpans(recorder)
		require.Len(t, spans, 3, "every span ended exactly once")

		var errored int
		for _, s := range spans {
			if s.Status().Code == codes.Error {
				errored++
			}
		}
		assert.Equal(t, 1, errored)
	})

	t.Run("span status follows each message's confirmation", func(t *testing.T) {
		recorder := setupTelemetry(t)
		cfg := baseConfig()
		cfg.PublisherConfirms = true
		cfg.ConfirmTimeout = time.Nanosecond // confirmations time out: per-message errors
		eb, err := NewEventBus(cfg)
		require.NoError(t, err)
		defer func() { _ = eb.Close() }()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "c"),
			newTestEvent("test.exchange", "test.event", "d"),
		}
		result, err := eb.PublishBatch(context.Background(), events, config.WithPipelining(true))
		require.NoError(t, err)

		spans := publishSpans(recorder)
		require.Len(t, spans, 2)

		var errored int
		for _, s := range spans {
			if s.Status().Code == codes.Error {
				errored++
			}
		}
		// Each failed confirmation (msgErr) is recorded on its own span, whatever the timing
		assert.Equal(t, result.Failed, errored)
	})

	t.Run("fatal error ends every span with error status", func(t *testing.T) {
		recorder := setupTelemetry(t)
		eb, err := NewEventBus(baseConfig())
		require.NoError(t, err)
		_ = eb.Close()

		events := []Event{
			newTestEvent("test.exchange", "test.event", "e"),
			newTestEvent("test.exchange", "test.event", "f"),
		}
		_, err = eb.PublishBatch(context.Background(), events, config.WithPipelining(true))
		require.Error(t, err)

		spans := publishSpans(recorder)
		require.Len(t, spans, 2)
		for _, s := range spans {
			assert.Equal(t, codes.Error, s.Status().Code)
		}
	})
}

// failingHandler fails, as a permanent error so it is not retried, for the message it watches.
type failingHandler struct {
	message string
}

func (h *failingHandler) Execute(msg *router.MessageContext) error {
	var body struct {
		Message string `json:"message"`
	}
	if err := msg.BindJSON(&body); err != nil {
		return err
	}
	if body.Message != h.message {
		return nil
	}
	return kiterrors.NewPermanentError(errors.New("handler failed"))
}

func TestConsumerSpanErrorStatus_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	recorder := setupTelemetry(t)
	eb, err := NewEventBus(baseConfig())
	require.NoError(t, err)
	defer func() { _ = eb.Close() }()

	eb.RegisterHandler("test.event", &failingHandler{message: "fail-traced"})
	go func() { _ = eb.StartConsume("test.queue", 1) }()
	time.Sleep(100 * time.Millisecond)

	reqCtx, reqSpan := otel.Tracer("test").Start(context.Background(), "POST /fail")
	require.NoError(t, eb.Publish(reqCtx, newTestEvent("test.exchange", "test.event", "fail-traced")))
	reqSpan.End()

	assert.Eventually(t, func() bool {
		for _, s := range recorder.Ended() {
			if s.SpanKind() == trace.SpanKindConsumer &&
				s.SpanContext().TraceID() == reqSpan.SpanContext().TraceID() {
				return s.Status().Code == codes.Error
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)
}
