package rabbitmq

import (
	"context"

	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/internal/broker"
	"github.com/edaniel30/rabbitmq-kit-go/internal/tracing"
	"go.opentelemetry.io/otel/trace"
)

// Publish publishes a single event to RabbitMQ.
//
// The event's Exchange() and Type() methods determine where the message
// is published. The ToMap() method is used to serialize the event to JSON.
//
// Example:
//
//	event := NewUserCreatedEvent(user.ID, user.Name, user.Email)
//	err := eventBus.Publish(ctx, event)
//
// With OpenTelemetry set up in the application, it records a PRODUCER span and carries the
// trace context in the message headers, so the consumer's span joins the same trace.
func (b *EventBus) Publish(ctx context.Context, event Event) (err error) {
	ctx, span := tracing.StartPublish(ctx, event.Exchange(), event.Type())
	defer func() { tracing.End(span, err) }()

	msg, err := createDefaultPublishing(ctx, event)
	if err != nil {
		return err
	}

	// Publish to RabbitMQ
	return b.publisher.Publish(ctx, event.Exchange(), event.Type(), msg)
}

// BatchResult contains the result of a batch publish operation.
type BatchResult struct {
	Total   int          // Total number of events in the batch
	Success int          // Number of successfully published events
	Failed  int          // Number of failed events
	Errors  []BatchError // Details of failed events
}

// BatchError represents a failed event in a batch publish operation.
type BatchError struct {
	Index int   // Index of the event in the original batch
	Event Event // The event that failed
	Error error // The error that occurred
}

// PublishBatch publishes multiple events with optimized pipelining.
// By default, this method uses pipelining for maximum throughput: all messages
// are sent first without waiting for confirmations, then all confirmations are
// collected. This is 5-10x faster than sequential publishing for large batches.
//
// The method returns a BatchResult with detailed information about successes
// and failures. By default, all events are attempted even if some fail.
//
// Options:
//   - WithPipelining(false): Use sequential publishing (legacy behavior)
//   - WithFailFast(true): Stop at the first error
//
// Examples:
//
//	// Fast pipelining mode (default)
//	result, err := eventBus.PublishBatch(ctx, events)
//	if result.Failed > 0 {
//	    for _, batchErr := range result.Errors {
//	        log.Printf("Event %d failed: %v", batchErr.Index, batchErr.Error)
//	    }
//	}
//
//	// Legacy sequential mode with fail-fast
//	result, err := eventBus.PublishBatch(ctx, events,
//	    WithPipelining(false),
//	    WithFailFast(true),
//	)
func (b *EventBus) PublishBatch(ctx context.Context, events []Event, opts ...config.BatchOption) (*BatchResult, error) {
	if len(events) == 0 {
		return &BatchResult{Total: 0}, nil
	}

	// Apply options
	cfg := config.DefaultBatchConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	result := &BatchResult{
		Total:  len(events),
		Errors: []BatchError{},
	}

	// Use pipelining if enabled and publisher confirms are on
	if cfg.UsePipelining {
		return b.publishBatchPipeline(ctx, events, cfg, result)
	}

	// Sequential mode (legacy)
	return b.publishBatchSequential(ctx, events, cfg, result)
}

// publishBatchPipeline uses pipelining for maximum throughput.
func (b *EventBus) publishBatchPipeline(ctx context.Context, events []Event, cfg config.BatchConfig, result *BatchResult) (*BatchResult, error) {
	// One PRODUCER span per event, ended once its confirmation (or error) is known
	spans := make([]trace.Span, len(events))
	endSpan := func(i int, err error) {
		if spans[i] != nil {
			tracing.End(spans[i], err)
			spans[i] = nil
		}
	}
	defer func() {
		for i := range spans {
			endSpan(i, nil)
		}
	}()

	// Serialize all events first
	messages := make([]broker.PublishMessage, len(events))
	for i, event := range events {
		msgCtx, span := tracing.StartPublish(ctx, event.Exchange(), event.Type())
		spans[i] = span

		msg, err := createDefaultPublishing(msgCtx, event)
		if err != nil {
			endSpan(i, err)
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Index: i,
				Event: event,
				Error: err,
			})
			if cfg.FailFast {
				return result, err
			}
			continue
		}

		messages[i] = broker.PublishMessage{
			Exchange:   event.Exchange(),
			RoutingKey: event.Type(),
			Message:    msg,
		}
	}

	// Publish all messages using pipelining
	messageErrors, err := b.publisher.PublishBatchPipeline(ctx, messages)
	if err != nil {
		// Fatal error (connection lost, etc.)
		for i := range spans {
			endSpan(i, err)
		}
		return result, err
	}

	// Process results
	for i, msgErr := range messageErrors {
		endSpan(i, msgErr)
		if msgErr != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Index: i,
				Event: events[i],
				Error: msgErr,
			})
			if cfg.FailFast {
				return result, msgErr
			}
		} else {
			result.Success++
		}
	}

	return result, nil
}

// publishBatchSequential uses sequential publishing (legacy mode).
func (b *EventBus) publishBatchSequential(ctx context.Context, events []Event, cfg config.BatchConfig, result *BatchResult) (*BatchResult, error) {
	for i, event := range events {
		if err := b.Publish(ctx, event); err != nil {
			result.Failed++
			result.Errors = append(result.Errors, BatchError{
				Index: i,
				Event: event,
				Error: err,
			})
			if cfg.FailFast {
				return result, err
			}
		} else {
			result.Success++
		}
	}
	return result, nil
}
