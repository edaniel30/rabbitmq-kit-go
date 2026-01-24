package rabbitmq

import (
	"context"
)

// Logger is the interface that any logger implementation must satisfy.
// This allows the library to be agnostic about the logging implementation.
//
// You can provide your own logger implementation (zap, logrus, zerolog, etc.)
// or use the provided DefaultLogger from internal/logger.
//
// Example with custom logger:
//
//	type MyLogger struct {
//	    zapLogger *zap.Logger
//	}
//
//	func (l *MyLogger) Info(ctx context.Context, msg string, fields map[string]any) {
//	    l.zapLogger.Info(msg, zap.Any("fields", fields))
//	}
//	// ... implement other methods
//
//	client, _ := broker.New(
//	    config.DefaultConfig(),
//	    config.WithLogger(&MyLogger{zapLogger: zapLog}),
//	)
//
// Example with DefaultLogger:
//
//	import "github.com/edaniel30/rabbitmq-kit-go/internal/logger"
//
//	client, _ := broker.New(
//	    config.DefaultConfig(),
//	    config.WithLogger(logger.New()),
//	)
type Logger interface {
	// Info logs an informational message with optional fields
	Info(ctx context.Context, msg string, fields map[string]any)

	// Error logs an error message with optional fields
	Error(ctx context.Context, msg string, fields map[string]any)

	// Warn logs a warning message with optional fields
	Warn(ctx context.Context, msg string, fields map[string]any)

	// Debug logs a debug message with optional fields
	Debug(ctx context.Context, msg string, fields map[string]any)

	// Close closes the logger and flushes any pending logs.
	// Returns an error if the logger fails to close or flush properly.
	Close() error
}
