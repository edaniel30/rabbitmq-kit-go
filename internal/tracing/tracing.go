// Package tracing adds OpenTelemetry spans to publishing and consuming, and carries the
// trace context in the message headers (W3C traceparent), so a consumer's work joins the
// trace of the request that published the event.
//
// It uses the global TracerProvider and TextMapPropagator. When the application has not set
// them (no telemetry), both are no-ops: no spans are recorded and no headers are added.
package tracing

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope of the spans.
const TracerName = "github.com/edaniel30/rabbitmq-kit-go"

const messagingSystem = "rabbitmq"

// HeaderCarrier adapts AMQP headers to OpenTelemetry's TextMapCarrier.
type HeaderCarrier amqp.Table

var _ propagation.TextMapCarrier = HeaderCarrier{}

// Get returns the value of a header, or "" if it is missing or not a string.
func (c HeaderCarrier) Get(key string) string {
	switch v := c[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

// Set stores a header.
func (c HeaderCarrier) Set(key, value string) {
	c[key] = value
}

// Keys lists the header names.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

func tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// StartPublish starts a PRODUCER span for a message published to exchange with routingKey.
// The returned context carries the span; inject it into the message with Inject.
func StartPublish(ctx context.Context, exchange, routingKey string) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return tracer().Start(ctx, fmt.Sprintf("publish %s", routingKey),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", messagingSystem),
			attribute.String("messaging.operation.type", "send"),
			attribute.String("messaging.destination.name", exchange),
			attribute.String("messaging.rabbitmq.destination.routing_key", routingKey),
		),
	)
}

// Inject writes the trace context of ctx (traceparent, tracestate, baggage) into headers.
func Inject(ctx context.Context, headers amqp.Table) {
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(headers))
}

// StartConsume extracts the trace context from the delivery's headers and starts a CONSUMER
// span, child of the publisher's span. The returned context carries the span and is the one
// handlers receive through MessageContext.Context().
func StartConsume(delivery amqp.Delivery, queue, eventType string) (context.Context, trace.Span) {
	ctx := context.Background()
	if delivery.Headers != nil {
		ctx = otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(delivery.Headers))
	}

	name := eventType
	if name == "" {
		name = delivery.RoutingKey
	}

	return tracer().Start(ctx, fmt.Sprintf("process %s", name),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", messagingSystem),
			attribute.String("messaging.operation.type", "process"),
			attribute.String("messaging.destination.name", delivery.Exchange),
			attribute.String("messaging.rabbitmq.destination.routing_key", delivery.RoutingKey),
			attribute.String("messaging.consumer.group.name", queue),
			attribute.String("messaging.message.id", delivery.MessageId),
		),
	)
}

// End records err on the span (status Error) and ends it.
func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// TraceID returns the trace ID of the span in ctx, or "" if there is none.
func TraceID(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}
