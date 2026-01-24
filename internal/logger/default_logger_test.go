package logger

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("creates logger with default settings", func(t *testing.T) {
		logger := New()

		require.NotNil(t, logger)
		require.NotNil(t, logger.logger)
	})
}

func TestNewTestLogger(t *testing.T) {
	t.Run("creates logger with custom log.Logger", func(t *testing.T) {
		var buf bytes.Buffer
		customLogger := log.New(&buf, "[custom] ", 0)

		logger := NewTestLogger(customLogger)

		require.NotNil(t, logger)
		assert.Equal(t, customLogger, logger.logger)
	})
}

func TestDefaultLogger_LogMethods(t *testing.T) {
	var buf bytes.Buffer
	testLogger := log.New(&buf, "[test] ", 0)
	logger := NewTestLogger(testLogger)
	ctx := context.Background()

	t.Run("Info logs message with fields", func(t *testing.T) {
		buf.Reset()
		logger.Info(ctx, "test info message", map[string]any{"key": "value"})

		output := buf.String()
		assert.Contains(t, output, "INFO")
		assert.Contains(t, output, "test info message")
		assert.Contains(t, output, "key=value")
	})

	t.Run("Info logs message without fields", func(t *testing.T) {
		buf.Reset()
		logger.Info(ctx, "simple info", nil)

		output := buf.String()
		assert.Contains(t, output, "INFO")
		assert.Contains(t, output, "simple info")
	})

	t.Run("Error logs message with fields", func(t *testing.T) {
		buf.Reset()
		logger.Error(ctx, "test error message", map[string]any{"error": "failed"})

		output := buf.String()
		assert.Contains(t, output, "ERROR")
		assert.Contains(t, output, "test error message")
		assert.Contains(t, output, "error=failed")
	})

	t.Run("Warn logs message with fields", func(t *testing.T) {
		buf.Reset()
		logger.Warn(ctx, "test warning message", map[string]any{"status": "warning"})

		output := buf.String()
		assert.Contains(t, output, "WARN")
		assert.Contains(t, output, "test warning message")
		assert.Contains(t, output, "status=warning")
	})

	t.Run("Debug logs message with fields", func(t *testing.T) {
		buf.Reset()
		logger.Debug(ctx, "test debug message", map[string]any{"debug": true})

		output := buf.String()
		assert.Contains(t, output, "DEBUG")
		assert.Contains(t, output, "test debug message")
		assert.Contains(t, output, "debug=true")
	})

	t.Run("logs with multiple fields sorted", func(t *testing.T) {
		buf.Reset()
		logger.Info(ctx, "multi field test", map[string]any{
			"z_field": "last",
			"a_field": "first",
			"m_field": "middle",
		})

		output := buf.String()
		assert.Contains(t, output, "INFO")
		// Check fields are present and sorted
		aIdx := strings.Index(output, "a_field=first")
		mIdx := strings.Index(output, "m_field=middle")
		zIdx := strings.Index(output, "z_field=last")
		assert.True(t, aIdx < mIdx && mIdx < zIdx, "fields should be sorted alphabetically")
	})
}

func TestDefaultLogger_Close(t *testing.T) {
	t.Run("Close returns no error", func(t *testing.T) {
		logger := New()
		err := logger.Close()

		assert.NoError(t, err)
	})
}

func TestFormatFields(t *testing.T) {
	tests := []struct {
		name     string
		fields   map[string]any
		expected string
	}{
		{
			name:     "empty fields",
			fields:   map[string]any{},
			expected: "",
		},
		{
			name: "single field",
			fields: map[string]any{
				"key": "value",
			},
			expected: "key=value",
		},
		{
			name: "multiple fields sorted",
			fields: map[string]any{
				"status": 200,
				"method": "GET",
				"path":   "/users",
			},
			expected: "method=GET path=/users status=200",
		},
		{
			name: "fields with various types",
			fields: map[string]any{
				"string": "value",
				"int":    123,
				"bool":   true,
				"float":  45.67,
			},
			expected: "bool=true float=45.67 int=123 string=value",
		},
		{
			name: "fields sorted alphabetically",
			fields: map[string]any{
				"zebra": "z",
				"apple": "a",
				"mango": "m",
			},
			expected: "apple=a mango=m zebra=z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatFields(tt.fields)
			assert.Equal(t, tt.expected, result)
		})
	}
}
