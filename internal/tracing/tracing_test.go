package tracing

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// setupTelemetry installs a recording TracerProvider and the W3C propagator, as an
// application with telemetry does, and restores the previous globals at the end of the test.
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

func TestHeaderCarrier(t *testing.T) {
	c := HeaderCarrier(amqp.Table{"a": "1", "b": []byte("2"), "n": int32(3)})
	assert.Equal(t, "1", c.Get("a"))
	assert.Equal(t, "2", c.Get("b"))
	assert.Empty(t, c.Get("n"))
	assert.Empty(t, c.Get("missing"))

	c.Set("traceparent", "x")
	assert.Equal(t, "x", c.Get("traceparent"))
	assert.ElementsMatch(t, []string{"a", "b", "n", "traceparent"}, c.Keys())
}

func TestPublishConsumeSameTrace(t *testing.T) {
	recorder := setupTelemetry(t)

	// The request that publishes
	reqCtx, reqSpan := otel.Tracer("test").Start(context.Background(), "GET /orders")

	pubCtx, pubSpan := StartPublish(reqCtx, "orders.exchange", "order.created")
	headers := amqp.Table{}
	Inject(pubCtx, headers)
	End(pubSpan, nil)
	reqSpan.End()

	require.NotEmpty(t, headers["traceparent"])

	delivery := amqp.Delivery{
		Headers:    headers,
		Exchange:   "orders.exchange",
		RoutingKey: "order.created",
		Type:       "order.created",
	}
	consCtx, consSpan := StartConsume(delivery, "orders.queue", delivery.Type)
	End(consSpan, errors.New("boom"))

	reqTraceID := reqSpan.SpanContext().TraceID().String()
	assert.Equal(t, reqTraceID, TraceID(pubCtx))
	assert.Equal(t, reqTraceID, TraceID(consCtx))

	spans := recorder.Ended()
	require.Len(t, spans, 3)
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byName[s.Name()] = s
	}

	pub := byName["publish order.created"]
	cons := byName["process order.created"]
	require.NotNil(t, pub)
	require.NotNil(t, cons)

	assert.Equal(t, trace.SpanKindProducer, pub.SpanKind())
	assert.Equal(t, reqSpan.SpanContext().SpanID(), pub.Parent().SpanID())
	assert.Equal(t, trace.SpanKindConsumer, cons.SpanKind())
	assert.Equal(t, pub.SpanContext().SpanID(), cons.Parent().SpanID())
	assert.Equal(t, codes.Error, cons.Status().Code)
	assert.Equal(t, codes.Unset, pub.Status().Code)
}

func TestWithoutTelemetry(t *testing.T) {
	// Default globals: no-op provider and propagator
	ctx, span := StartPublish(context.Background(), "ex", "rk")
	headers := amqp.Table{}
	Inject(ctx, headers)
	End(span, nil)

	assert.Empty(t, headers)
	assert.Empty(t, TraceID(ctx))

	consCtx, consSpan := StartConsume(amqp.Delivery{RoutingKey: "rk"}, "q", "")
	End(consSpan, nil)
	assert.Empty(t, TraceID(consCtx))
}

func TestStartPublishNilContext(t *testing.T) {
	//nolint:staticcheck // nil context is handled on purpose
	ctx, span := StartPublish(nil, "ex", "rk")
	End(span, nil)
	assert.NotNil(t, ctx)
}

// An expected retry leaves the span without error status: an event and an
// attribute record it instead.
func TestMarkExpectedRetry(t *testing.T) {
	recorder := setupTelemetry(t)

	_, span := StartConsume(amqp.Delivery{RoutingKey: "billing.file.added"}, "billing.queue", "billing.file.added")
	MarkExpectedRetry(span, errors.New("billing version conflict"))
	End(span, nil)

	ended := recorder.Ended()
	require.Len(t, ended, 1)
	assert.Equal(t, codes.Unset, ended[0].Status().Code)

	var expected bool
	for _, attr := range ended[0].Attributes() {
		if attr.Key == "messaging.rabbitmq.expected_retry" {
			expected = attr.Value.AsBool()
		}
	}
	assert.True(t, expected)

	require.Len(t, ended[0].Events(), 1)
	assert.Equal(t, "expected retry", ended[0].Events()[0].Name)
}
