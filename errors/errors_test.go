package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSentinelErrors(t *testing.T) {
	t.Run("sentinel errors are defined", func(t *testing.T) {
		assert.NotNil(t, ErrClientClosed)
		assert.NotNil(t, ErrNoConnection)
		assert.NotNil(t, ErrNoChannel)
		assert.NotNil(t, ErrMaxRetriesExceeded)
		assert.NotNil(t, ErrNoHandlersRegistered)
		assert.NotNil(t, ErrPublishNotConfirmed)
		assert.NotNil(t, ErrPublishConfirmTimeout)
	})

	t.Run("sentinel errors have correct messages", func(t *testing.T) {
		assert.Contains(t, ErrClientClosed.Error(), "client is closed")
		assert.Contains(t, ErrNoConnection.Error(), "no active connection")
		assert.Contains(t, ErrNoChannel.Error(), "no active channel")
		assert.Contains(t, ErrMaxRetriesExceeded.Error(), "max retries exceeded")
		assert.Contains(t, ErrNoHandlersRegistered.Error(), "no handlers registered")
		assert.Contains(t, ErrPublishNotConfirmed.Error(), "not confirmed")
		assert.Contains(t, ErrPublishConfirmTimeout.Error(), "timeout")
	})
}

func TestConfigError(t *testing.T) {
	t.Run("NewConfigFieldError creates error", func(t *testing.T) {
		err := NewConfigFieldError("URI", "cannot be empty")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URI")
		assert.Contains(t, err.Error(), "cannot be empty")
	})

	t.Run("ConfigError Error method", func(t *testing.T) {
		err := &ConfigError{
			Field:   "PrefetchCount",
			Message: "must be positive",
		}
		errMsg := err.Error()
		assert.Contains(t, errMsg, "config error")
		assert.Contains(t, errMsg, "PrefetchCount")
		assert.Contains(t, errMsg, "must be positive")
	})

	t.Run("ConfigError can be unwrapped", func(t *testing.T) {
		err := NewConfigFieldError("Timeout", "invalid")
		var configErr *ConfigError
		assert.True(t, errors.As(err, &configErr))
		assert.Equal(t, "Timeout", configErr.Field)
		assert.Equal(t, "invalid", configErr.Message)
	})
}

func TestConnectionError(t *testing.T) {
	t.Run("NewConnectionError with cause", func(t *testing.T) {
		cause := errors.New("connection refused")
		err := NewConnectionError("dial", cause)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection error")
		assert.Contains(t, err.Error(), "dial")
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("NewConnectionError without cause", func(t *testing.T) {
		err := NewConnectionError("connect", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection error")
		assert.Contains(t, err.Error(), "connect")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ConnectionError Unwrap", func(t *testing.T) {
		cause := errors.New("network error")
		err := NewConnectionError("reconnect", cause)
		unwrapped := errors.Unwrap(err)
		assert.Equal(t, cause, unwrapped)
	})

	t.Run("ConnectionError type assertion", func(t *testing.T) {
		err := NewConnectionError("test", nil)
		var connErr *ConnectionError
		assert.True(t, errors.As(err, &connErr))
		assert.Equal(t, "test", connErr.Operation)
	})
}

func TestPublishError(t *testing.T) {
	t.Run("NewPublishError with cause", func(t *testing.T) {
		cause := errors.New("channel closed")
		err := NewPublishError("events.exchange", "user.created", cause)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "publish failed")
		assert.Contains(t, err.Error(), "events.exchange")
		assert.Contains(t, err.Error(), "user.created")
		assert.Contains(t, err.Error(), "channel closed")
	})

	t.Run("NewPublishError without cause", func(t *testing.T) {
		err := NewPublishError("test.exchange", "test.key", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "publish failed")
		assert.Contains(t, err.Error(), "test.exchange")
		assert.Contains(t, err.Error(), "test.key")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("PublishError Unwrap", func(t *testing.T) {
		cause := errors.New("timeout")
		err := NewPublishError("ex", "key", cause)
		unwrapped := errors.Unwrap(err)
		assert.Equal(t, cause, unwrapped)
	})

	t.Run("PublishError type assertion", func(t *testing.T) {
		err := NewPublishError("my.exchange", "my.key", nil)
		var pubErr *PublishError
		assert.True(t, errors.As(err, &pubErr))
		assert.Equal(t, "my.exchange", pubErr.Exchange)
		assert.Equal(t, "my.key", pubErr.RoutingKey)
	})
}

func TestConsumeError(t *testing.T) {
	t.Run("NewConsumeError with cause", func(t *testing.T) {
		cause := errors.New("queue not found")
		err := NewConsumeError("payments.queue", cause)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consume failed")
		assert.Contains(t, err.Error(), "payments.queue")
		assert.Contains(t, err.Error(), "queue not found")
	})

	t.Run("NewConsumeError without cause", func(t *testing.T) {
		err := NewConsumeError("test.queue", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consume failed")
		assert.Contains(t, err.Error(), "test.queue")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("ConsumeError Unwrap", func(t *testing.T) {
		cause := errors.New("consumer cancelled")
		err := NewConsumeError("orders", cause)
		unwrapped := errors.Unwrap(err)
		assert.Equal(t, cause, unwrapped)
	})

	t.Run("ConsumeError type assertion", func(t *testing.T) {
		err := NewConsumeError("my.queue", nil)
		var consumeErr *ConsumeError
		assert.True(t, errors.As(err, &consumeErr))
		assert.Equal(t, "my.queue", consumeErr.Queue)
	})
}

func TestTopologyError(t *testing.T) {
	t.Run("NewTopologyError with cause", func(t *testing.T) {
		cause := errors.New("permission denied")
		err := NewTopologyError("declare_exchange", "events.exchange", cause)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topology error")
		assert.Contains(t, err.Error(), "declare_exchange")
		assert.Contains(t, err.Error(), "events.exchange")
		assert.Contains(t, err.Error(), "permission denied")
	})

	t.Run("NewTopologyError without cause", func(t *testing.T) {
		err := NewTopologyError("bind_queue", "test.queue", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "topology error")
		assert.Contains(t, err.Error(), "bind_queue")
		assert.Contains(t, err.Error(), "test.queue")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("TopologyError Unwrap", func(t *testing.T) {
		cause := errors.New("invalid arguments")
		err := NewTopologyError("declare_queue", "orders", cause)
		unwrapped := errors.Unwrap(err)
		assert.Equal(t, cause, unwrapped)
	})

	t.Run("TopologyError type assertion", func(t *testing.T) {
		err := NewTopologyError("bind_queue", "my.queue", nil)
		var topoErr *TopologyError
		assert.True(t, errors.As(err, &topoErr))
		assert.Equal(t, "bind_queue", topoErr.Operation)
		assert.Equal(t, "my.queue", topoErr.Resource)
	})
}

func TestHandlerError(t *testing.T) {
	t.Run("NewHandlerError with cause", func(t *testing.T) {
		cause := errors.New("database connection failed")
		err := NewHandlerError("user.created", "msg-123", cause)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler error")
		assert.Contains(t, err.Error(), "user.created")
		assert.Contains(t, err.Error(), "msg-123")
		assert.Contains(t, err.Error(), "database connection failed")
	})

	t.Run("NewHandlerError without cause", func(t *testing.T) {
		err := NewHandlerError("order.processed", "msg-456", nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "handler error")
		assert.Contains(t, err.Error(), "order.processed")
		assert.Contains(t, err.Error(), "msg-456")
		assert.NotContains(t, err.Error(), "<nil>")
	})

	t.Run("HandlerError Unwrap", func(t *testing.T) {
		cause := errors.New("validation failed")
		err := NewHandlerError("payment.received", "msg-789", cause)
		unwrapped := errors.Unwrap(err)
		assert.Equal(t, cause, unwrapped)
	})

	t.Run("HandlerError type assertion", func(t *testing.T) {
		err := NewHandlerError("test.event", "test-id", nil)
		var handlerErr *HandlerError
		assert.True(t, errors.As(err, &handlerErr))
		assert.Equal(t, "test.event", handlerErr.EventType)
		assert.Equal(t, "test-id", handlerErr.MessageID)
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Run("errors.Is works with ConnectionError", func(t *testing.T) {
		cause := errors.New("timeout")
		err := NewConnectionError("dial", cause)
		assert.True(t, errors.Is(err, cause))
	})

	t.Run("errors.Is works with PublishError", func(t *testing.T) {
		cause := ErrClientClosed
		err := NewPublishError("ex", "key", cause)
		assert.True(t, errors.Is(err, ErrClientClosed))
	})

	t.Run("errors.Is works with TopologyError", func(t *testing.T) {
		cause := ErrNoConnection
		err := NewTopologyError("declare", "queue", cause)
		assert.True(t, errors.Is(err, ErrNoConnection))
	})
}
