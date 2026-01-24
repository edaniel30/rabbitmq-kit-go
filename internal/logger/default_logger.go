package logger

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

// DefaultLogger is a simple logger implementation using Go's standard log package.
// It's used as the default logger when no custom logger is provided.
// This type is internal and should be created through rabbitmq.NewDefaultLogger().
type DefaultLogger struct {
	logger *log.Logger
}

// New creates a new default logger that writes to stdout.
// This is used internally by the public NewDefaultLogger function.
func New() *DefaultLogger {
	return &DefaultLogger{
		logger: log.New(os.Stdout, "[rabbitmq-kit] ", log.LstdFlags),
	}
}

// NewTestLogger creates a new logger with a custom log.Logger for testing purposes.
// This allows tests to capture log output to a buffer.
func NewTestLogger(l *log.Logger) *DefaultLogger {
	return &DefaultLogger{
		logger: l,
	}
}

// Info logs an informational message with optional fields.
func (l *DefaultLogger) Info(ctx context.Context, msg string, fields map[string]any) {
	l.log("INFO", msg, fields)
}

// Error logs an error message with optional fields.
func (l *DefaultLogger) Error(ctx context.Context, msg string, fields map[string]any) {
	l.log("ERROR", msg, fields)
}

// Warn logs a warning message with optional fields.
func (l *DefaultLogger) Warn(ctx context.Context, msg string, fields map[string]any) {
	l.log("WARN", msg, fields)
}

// Debug logs a debug message with optional fields.
func (l *DefaultLogger) Debug(ctx context.Context, msg string, fields map[string]any) {
	l.log("DEBUG", msg, fields)
}

// Close closes the logger. Since the standard log package doesn't require cleanup,
// this method is a no-op.
func (l *DefaultLogger) Close() error {
	return nil
}

// log is a helper method that formats and writes log messages.
func (l *DefaultLogger) log(level, msg string, fields map[string]any) {
	if len(fields) > 0 {
		l.logger.Printf("%s: %s %s", level, msg, FormatFields(fields))
	} else {
		l.logger.Printf("%s: %s", level, msg)
	}
}

// FormatFields converts fields map to a readable key=value string format.
// Exported for testing purposes.
func FormatFields(fields map[string]any) string {
	if len(fields) == 0 {
		return ""
	}

	// Sort keys for deterministic output
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build key=value pairs
	pairs := make([]string, 0, len(fields))
	for _, k := range keys {
		v := fields[k]
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, v))
	}

	return strings.Join(pairs, " ")
}
