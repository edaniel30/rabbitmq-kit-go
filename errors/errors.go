package errors

import (
	"errors"
	"fmt"
)

// Sentinel errors
var (
	// ErrClientClosed is returned when attempting to use a closed client.
	ErrClientClosed = errors.New("rabbitmq: client is closed")

	// ErrNoConnection is returned when the client has no active connection.
	ErrNoConnection = errors.New("rabbitmq: no active connection")

	// ErrNoChannel is returned when the client has no active channel.
	ErrNoChannel = errors.New("rabbitmq: no active channel")

	// ErrMaxRetriesExceeded is returned when a message exceeds max retries.
	ErrMaxRetriesExceeded = errors.New("rabbitmq: max retries exceeded")

	// ErrNoHandlersRegistered is returned when StartConsume is called without registering any handlers.
	ErrNoHandlersRegistered = errors.New("rabbitmq: no handlers registered, call RegisterHandler before StartConsume")

	// ErrPublishNotConfirmed is returned when a message publish is not confirmed by RabbitMQ.
	ErrPublishNotConfirmed = errors.New("rabbitmq: message publish was not confirmed by broker")

	// ErrPublishConfirmTimeout is returned when waiting for publish confirmation times out.
	ErrPublishConfirmTimeout = errors.New("rabbitmq: timeout waiting for publish confirmation")
)

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("rabbitmq: config error [%s]: %s", e.Field, e.Message)
}

// newConfigFieldError creates a new ConfigError for a specific field.
func NewConfigFieldError(field, message string) error {
	return &ConfigError{
		Field:   field,
		Message: message,
	}
}

// ConnectionError represents a connection-related error.
type ConnectionError struct {
	Operation string
	Cause     error
}

func (e *ConnectionError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("rabbitmq: connection error during '%s': %v", e.Operation, e.Cause)
	}
	return fmt.Sprintf("rabbitmq: connection error during '%s'", e.Operation)
}

func (e *ConnectionError) Unwrap() error {
	return e.Cause
}

// newConnectionError creates a new ConnectionError.
func NewConnectionError(operation string, cause error) error {
	return &ConnectionError{
		Operation: operation,
		Cause:     cause,
	}
}

// PublishError represents an error during message publishing.
type PublishError struct {
	Exchange   string
	RoutingKey string
	Cause      error
}

func (e *PublishError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("rabbitmq: publish failed [exchange=%s, routing_key=%s]: %v",
			e.Exchange, e.RoutingKey, e.Cause)
	}
	return fmt.Sprintf("rabbitmq: publish failed [exchange=%s, routing_key=%s]",
		e.Exchange, e.RoutingKey)
}

func (e *PublishError) Unwrap() error {
	return e.Cause
}

// newPublishError creates a new PublishError.
func NewPublishError(exchange, routingKey string, cause error) error {
	return &PublishError{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Cause:      cause,
	}
}

// ConsumeError represents an error during message consumption.
type ConsumeError struct {
	Queue string
	Cause error
}

func (e *ConsumeError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("rabbitmq: consume failed [queue=%s]: %v", e.Queue, e.Cause)
	}
	return fmt.Sprintf("rabbitmq: consume failed [queue=%s]", e.Queue)
}

func (e *ConsumeError) Unwrap() error {
	return e.Cause
}

// newConsumeError creates a new ConsumeError.
func NewConsumeError(queue string, cause error) error {
	return &ConsumeError{
		Queue: queue,
		Cause: cause,
	}
}

// TopologyError represents an error during topology setup (exchanges, queues, bindings).
type TopologyError struct {
	Operation string // "declare_exchange", "declare_queue", "bind_queue"
	Resource  string // name of the exchange/queue
	Cause     error
}

func (e *TopologyError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("rabbitmq: topology error during '%s' [resource=%s]: %v",
			e.Operation, e.Resource, e.Cause)
	}
	return fmt.Sprintf("rabbitmq: topology error during '%s' [resource=%s]",
		e.Operation, e.Resource)
}

func (e *TopologyError) Unwrap() error {
	return e.Cause
}

// newTopologyError creates a new TopologyError.
func NewTopologyError(operation, resource string, cause error) error {
	return &TopologyError{
		Operation: operation,
		Resource:  resource,
		Cause:     cause,
	}
}

// HandlerError represents an error returned by a message handler.
type HandlerError struct {
	EventType string
	MessageID string
	Cause     error
}

func (e *HandlerError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("rabbitmq: handler error [type=%s, message_id=%s]: %v",
			e.EventType, e.MessageID, e.Cause)
	}
	return fmt.Sprintf("rabbitmq: handler error [type=%s, message_id=%s]",
		e.EventType, e.MessageID)
}

func (e *HandlerError) Unwrap() error {
	return e.Cause
}

// newHandlerError creates a new HandlerError.
func NewHandlerError(eventType, messageID string, cause error) error {
	return &HandlerError{
		EventType: eventType,
		MessageID: messageID,
		Cause:     cause,
	}
}

// ExpectedRetryError marks a handler error as an expected, self-healing
// condition (e.g. an optimistic-concurrency conflict from two workers
// racing on the same document) rather than a failure worth investigating.
// The consumer still retries it exactly like any other handler error - the
// only difference is how it's logged (see NewExpectedRetryError).
type ExpectedRetryError struct {
	Cause error
}

func (e *ExpectedRetryError) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return "rabbitmq: expected retry"
}

func (e *ExpectedRetryError) Unwrap() error {
	return e.Cause
}

// NewExpectedRetryError wraps cause so the consumer logs the retry at Warn
// (no stack trace) instead of Error. Use this for errors a handler expects
// to routinely resolve on retry (optimistic-concurrency conflicts between
// concurrent workers, for example) - as opposed to unexpected failures that
// genuinely warrant investigation.
func NewExpectedRetryError(cause error) error {
	if cause == nil {
		return nil
	}
	return &ExpectedRetryError{Cause: cause}
}

// IsExpectedRetry reports whether err (or any error it wraps) was marked via
// NewExpectedRetryError.
func IsExpectedRetry(err error) bool {
	var e *ExpectedRetryError
	return errors.As(err, &e)
}
