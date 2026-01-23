package logger

import (
	"log"
	"os"
)

// Logger is the interface for logging within the RabbitMQ client.
type Logger interface {
	// Info logs informational messages (connection status, topology setup, etc.)
	Info(msg string, args ...any)

	// Error logs error messages (connection failures, publish errors, etc.)
	Error(msg string, args ...any)

	// Warn logs warning messages (retry attempts, unexpected confirmations, etc.)
	Warn(msg string, args ...any)

	// Debug logs debug messages (message processing details, internal state, etc.)
	Debug(msg string, args ...any)
}

// DefaultLogger is the default logger implementation using Go's standard log package.
type DefaultLogger struct {
	logger *log.Logger
}

// NewDefaultLogger creates a new DefaultLogger instance.
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		logger: log.New(os.Stderr, "", log.LstdFlags),
	}
}

// Info logs an informational message.
func (l *DefaultLogger) Info(msg string, args ...any) {
	l.logger.Printf("[RabbitMQ] [INFO] "+msg, args...)
}

// Error logs an error message.
func (l *DefaultLogger) Error(msg string, args ...any) {
	l.logger.Printf("[RabbitMQ] [ERROR] "+msg, args...)
}

// Warn logs a warning message.
func (l *DefaultLogger) Warn(msg string, args ...any) {
	l.logger.Printf("[RabbitMQ] [WARN] "+msg, args...)
}

// Debug logs a debug message.
func (l *DefaultLogger) Debug(msg string, args ...any) {
	l.logger.Printf("[RabbitMQ] [DEBUG] "+msg, args...)
}

// NoopLogger is a logger that discards all log messages.
type NoopLogger struct{}

// Info does nothing.
func (l *NoopLogger) Info(msg string, args ...any) {}

// Error does nothing.
func (l *NoopLogger) Error(msg string, args ...any) {}

// Warn does nothing.
func (l *NoopLogger) Warn(msg string, args ...any) {}

// Debug does nothing.
func (l *NoopLogger) Debug(msg string, args ...any) {}
