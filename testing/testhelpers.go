package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RabbitMQContainer wraps a testcontainers RabbitMQ instance.
// It provides helper methods to get connection details for integration tests.
type RabbitMQContainer struct {
	testcontainers.Container
	URI      string
	Host     string
	Port     string
	Username string
	Password string
}

// SetupRabbitMQContainer starts a RabbitMQ container for integration testing.
//
// The container is configured with:
//   - RabbitMQ 3.13 with management plugin
//   - Default credentials: guest/guest
//   - Ports: 5672 (AMQP), 15672 (Management UI)
//   - 2-minute startup timeout
//
// Usage:
//
//	func TestIntegration(t *testing.T) {
//	    if testing.Short() {
//	        t.Skip("skipping integration test")
//	    }
//
//	    container := SetupRabbitMQContainer(t)
//	    defer container.Teardown(t)
//
//	    // Use container.URI for connection
//	}
func SetupRabbitMQContainer(t *testing.T) *RabbitMQContainer {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.13-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Server startup complete"),
			wait.ForListeningPort("5672/tcp"),
		).WithDeadline(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start RabbitMQ container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5672")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	uri := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())

	return &RabbitMQContainer{
		Container: container,
		URI:       uri,
		Host:      host,
		Port:      port.Port(),
		Username:  "guest",
		Password:  "guest",
	}
}

// Teardown terminates the RabbitMQ container and cleans up resources.
//
// This should be called with defer immediately after SetupRabbitMQContainer:
//
//	container := SetupRabbitMQContainer(t)
//	defer container.Teardown(t)
func (c *RabbitMQContainer) Teardown(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.Terminate(ctx); err != nil {
		t.Logf("failed to terminate RabbitMQ container: %v", err)
	}
}

// ManagementURL returns the URL for the RabbitMQ management UI.
//
// This can be used to verify queue state or debug test failures.
func (c *RabbitMQContainer) ManagementURL() string {
	return fmt.Sprintf("http://%s:15672", c.Host)
}
