// Circuit Breaker Pattern with rabbitmq-kit-go
//
// This example demonstrates:
// - Enabling circuit breaker to protect against cascading failures
// - Circuit breaker states: Closed -> Open -> Half-Open -> Closed
// - Monitoring circuit breaker metrics
// - Manual circuit breaker reset
//
// Prerequisites:
// - RabbitMQ running on localhost:5672
//
// Run with:
//   export RABBITMQ_URI="amqp://guest:guest@localhost:5672/"
//   go run main.go

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	rabbitmq "github.com/edaniel30/rabbitmq-kit-go"
	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/router"
)

// HealthCheckEvent represents a health check event
type HealthCheckEvent struct {
	ServiceName string    `json:"service_name"`
	Endpoint    string    `json:"endpoint"`
	Timestamp   time.Time `json:"timestamp"`
	ShouldFail  bool      `json:"should_fail"` // Explicit flag to control failure
}

func (e HealthCheckEvent) Type() string      { return "health.check" }
func (e HealthCheckEvent) Exchange() string  { return "health.exchange" }
func (e HealthCheckEvent) ToMap() map[string]any {
	return map[string]any{
		"type":         e.Type(),
		"service_name": e.ServiceName,
		"endpoint":     e.Endpoint,
		"timestamp":    e.Timestamp,
		"should_fail":  e.ShouldFail,
	}
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	// Create EventBus with circuit breaker enabled
	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithCircuitBreaker(true),                        // Enable circuit breaker
		config.WithCircuitBreakerMaxFailures(5),                // Open after 5 failures
		config.WithCircuitBreakerResetTimeout(10*time.Second),  // Try half-open after 10s
		config.WithCircuitBreakerHalfOpenRequests(3),           // Allow 3 requests in half-open
		config.WithMaxRetries(0),                               // Don't retry (to avoid old messages in queue)
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "health.exchange", Type: "direct", Durable: true},
		}),
		config.WithQueues([]config.QueueConfig{
			{
				Name:        "health.queue",
				Exchange:    "health.exchange",
				RoutingKeys: []string{"health.check"},
				Durable:     true,
			},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	log.Println("✅ Connected to RabbitMQ with Circuit Breaker")
	log.Println("   Max Failures: 5")
	log.Println("   Reset Timeout: 10s")
	log.Println("   Half-Open Requests: 3")
	log.Println("   Max Retries: 0 (messages discard on failure)")
	log.Println("")

	// Register handler that will fail intentionally
	eventBus.RegisterHandler("health.check", &HealthCheckHandler{})

	// Start consuming
	go func() {
		log.Println("📥 Starting consumer with circuit breaker...")
		if err := eventBus.StartConsume("health.queue", 2); err != nil {
			log.Fatalf("Failed to start consumer: %v", err)
		}
	}()

	// Monitor circuit breaker status
	go monitorCircuitBreaker(eventBus)

	time.Sleep(2 * time.Second)

	// Phase 1: Trigger failures to open circuit
	log.Println("\n📊 Phase 1: Triggering failures (should open circuit after 5 failures)")
	log.Println("=======================================================================")
	publishHealthChecks(eventBus, 10, true) // All will fail

	time.Sleep(3 * time.Second)

	// Phase 2: Wait for circuit to attempt half-open
	log.Println("\n⏰ Phase 2: Waiting for circuit to attempt half-open (10s)...")
	log.Println("================================================================")
	time.Sleep(11 * time.Second)

	// Phase 3: Successful messages in half-open (should close circuit)
	log.Println("\n✅ Phase 3: Sending successful messages (should close circuit)")
	log.Println("================================================================")
	publishHealthChecks(eventBus, 5, false) // shouldFail = false

	time.Sleep(3 * time.Second)

	// Phase 4: Demonstrate manual reset
	log.Println("\n🔄 Phase 4: Manual circuit breaker reset")
	log.Println("=========================================")
	if eventBus.ResetCircuitBreaker() {
		log.Println("✅ Circuit breaker manually reset to CLOSED")
	}

	// Wait for signal
	log.Println("\n⏳ Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
}

// HealthCheckHandler simulates a handler that may fail
type HealthCheckHandler struct{}

func (h *HealthCheckHandler) Execute(ctx *router.MessageContext) error {
	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	serviceName := data["service_name"].(string)
	shouldFail := data["should_fail"].(bool)

	// Simulate failure based on message flag
	if shouldFail {
		log.Printf("❌ Health check failed: %s", serviceName)
		return fmt.Errorf("simulated health check failure")
	}

	log.Printf("✅ Health check success: %s", serviceName)
	return nil
}

func publishHealthChecks(eventBus *rabbitmq.EventBus, count int, shouldFail bool) {
	for i := 0; i < count; i++ {
		event := HealthCheckEvent{
			ServiceName: "api-service",
			Endpoint:    "/health",
			Timestamp:   time.Now(),
			ShouldFail:  shouldFail,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, event)
		cancel()

		if err != nil {
			log.Printf("❌ Failed to publish: %v", err)
		}

		time.Sleep(300 * time.Millisecond)
	}
}

func monitorCircuitBreaker(eventBus *rabbitmq.EventBus) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		metrics := eventBus.GetCircuitBreakerMetrics()
		if metrics == nil {
			continue
		}

		log.Printf("\n🔌 Circuit Breaker Status:")
		log.Printf("   State: %s", metrics.State)
		log.Printf("   Failures: %d", metrics.Failures)
		log.Printf("   Successes: %d", metrics.Successes)

		if metrics.State.String() == "half-open" {
			log.Printf("   Half-Open Requests: %d/3", metrics.HalfOpenRequests)
		}

		if !metrics.LastFailureTime.IsZero() {
			log.Printf("   Last Failure: %v ago", time.Since(metrics.LastFailureTime).Round(time.Second))
		}
		log.Println("")
	}
}
