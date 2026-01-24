// Batch Publishing with rabbitmq-kit-go
//
// This example demonstrates:
// - Publishing multiple events in a batch with pipelining
// - PublishBatchAsync with worker pool control
// - Handling partial failures in batch operations
// - Performance comparison between sequential and batch publishing
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
	"log"
	"os"
	"time"

	rabbitmq "github.com/edaniel30/rabbitmq-kit-go"
	"github.com/edaniel30/rabbitmq-kit-go/config"
)

// MetricEvent represents a metric event
type MetricEvent struct {
	MetricName  string    `json:"metric_name"`
	Value       float64   `json:"value"`
	Timestamp   time.Time `json:"timestamp"`
	ServiceName string    `json:"service_name"`
}

func (e MetricEvent) Type() string     { return "metric.recorded" }
func (e MetricEvent) Exchange() string { return "metrics.exchange" }
func (e MetricEvent) ToMap() map[string]any {
	return map[string]any{
		"type":         e.Type(),
		"metric_name":  e.MetricName,
		"value":        e.Value,
		"timestamp":    e.Timestamp,
		"service_name": e.ServiceName,
	}
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	// Create EventBus with publisher confirms for batch operations
	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithPublisherConfirms(true),
		config.WithConfirmTimeout(10*time.Second),
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "metrics.exchange", Type: "fanout", Durable: true},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer func() { _ = eventBus.Close() }()

	log.Println("✅ Connected to RabbitMQ with Publisher Confirms")

	// Example 1: PublishBatch with pipelining (default)
	example1PipelinedBatch(eventBus)

	time.Sleep(2 * time.Second)

	// Example 2: PublishBatch sequential mode
	example2SequentialBatch(eventBus)

	time.Sleep(2 * time.Second)

	// Example 3: PublishBatchAsync with worker pool
	example3AsyncBatch(eventBus)

	time.Sleep(2 * time.Second)

	// Example 4: Handling partial failures
	example4PartialFailures(eventBus)

	log.Println("\n✅ All examples completed!")
}

// Example 1: Pipelined batch publishing (fastest)
func example1PipelinedBatch(eventBus *rabbitmq.EventBus) {
	log.Println("\n📊 Example 1: Pipelined Batch Publishing")
	log.Println("===========================================")

	events := generateMetricEvents(100)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Default mode uses pipelining
	result, err := eventBus.PublishBatch(ctx, events)
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("❌ Fatal error: %v", err)
		return
	}

	log.Printf("✅ Published %d/%d events in %v", result.Success, result.Total, elapsed)
	log.Printf("   Throughput: %.0f msg/s", float64(result.Success)/elapsed.Seconds())

	if result.Failed > 0 {
		log.Printf("⚠️  Failed: %d events", result.Failed)
	}
}

// Example 2: Sequential batch publishing (legacy mode)
func example2SequentialBatch(eventBus *rabbitmq.EventBus) {
	log.Println("\n📊 Example 2: Sequential Batch Publishing")
	log.Println("===========================================")

	events := generateMetricEvents(100)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Disable pipelining for comparison
	result, err := eventBus.PublishBatch(ctx, events, config.WithPipelining(false))
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("❌ Fatal error: %v", err)
		return
	}

	log.Printf("✅ Published %d/%d events in %v", result.Success, result.Total, elapsed)
	log.Printf("   Throughput: %.0f msg/s", float64(result.Success)/elapsed.Seconds())
	log.Printf("   (Note: Sequential mode is slower than pipelining)")
}

// Example 3: Async batch with worker pool
func example3AsyncBatch(eventBus *rabbitmq.EventBus) {
	log.Println("\n📊 Example 3: Async Batch with Worker Pool")
	log.Println("===========================================")

	events := generateMetricEvents(100)

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use 50 concurrent workers
	result, err := eventBus.PublishBatchAsync(ctx, events, config.WithMaxConcurrency(50))
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("❌ Fatal error: %v", err)
		return
	}

	log.Printf("✅ Published %d/%d events in %v", result.Success, result.Total, elapsed)
	log.Printf("   Throughput: %.0f msg/s", float64(result.Success)/elapsed.Seconds())
	log.Printf("   Workers: 50")
	log.Printf("   (Note: Events may be out of order)")
}

// Example 4: Handling partial failures
func example4PartialFailures(eventBus *rabbitmq.EventBus) {
	log.Println("\n📊 Example 4: Handling Partial Failures")
	log.Println("===========================================")

	events := generateMetricEvents(10)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := eventBus.PublishBatch(ctx, events)
	if err != nil {
		log.Printf("❌ Fatal error: %v", err)
		return
	}

	log.Printf("📈 Batch Result:")
	log.Printf("   Total:   %d", result.Total)
	log.Printf("   Success: %d", result.Success)
	log.Printf("   Failed:  %d", result.Failed)

	if result.Failed > 0 {
		log.Println("\n❌ Failed Events:")
		for _, batchErr := range result.Errors {
			log.Printf("   [%d] %s: %v",
				batchErr.Index,
				batchErr.Event.Type(),
				batchErr.Error,
			)
		}
	}
}

// Generate sample metric events
func generateMetricEvents(count int) []rabbitmq.Event {
	events := make([]rabbitmq.Event, count)
	services := []string{"api", "worker", "database", "cache", "queue"}

	for i := 0; i < count; i++ {
		events[i] = MetricEvent{
			MetricName:  "cpu.usage.percent",
			Value:       float64(i%100) + 0.5,
			Timestamp:   time.Now(),
			ServiceName: services[i%len(services)],
		}
	}

	return events
}
