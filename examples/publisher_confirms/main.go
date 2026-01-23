// Publisher Confirms with rabbitmq-kit-go
//
// This example demonstrates:
// - Enabling publisher confirms for guaranteed delivery
// - High-performance asynchronous confirmation system
// - Handling publish confirmations (ACK/NACK)
// - Timeout configuration for confirms
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
	"time"

	rabbitmq "github.com/edaniel30/rabbitmq-kit-go"
	"github.com/edaniel30/rabbitmq-kit-go/config"
)

// TransactionEvent represents a financial transaction
type TransactionEvent struct {
	TxID      string    `json:"tx_id"`
	Amount    float64   `json:"amount"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Timestamp time.Time `json:"timestamp"`
}

func (e TransactionEvent) Type() string      { return "transaction.created" }
func (e TransactionEvent) Exchange() string  { return "transactions.exchange" }
func (e TransactionEvent) ToMap() map[string]any {
	return map[string]any{
		"type":      e.Type(),
		"tx_id":     e.TxID,
		"amount":    e.Amount,
		"from":      e.From,
		"to":        e.To,
		"timestamp": e.Timestamp,
	}
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	log.Println("📊 Publisher Confirms Demonstration")
	log.Println("====================================\n")

	// Example 1: Without publisher confirms (fire and forget)
	example1WithoutConfirms(rabbitURI)

	time.Sleep(2 * time.Second)

	// Example 2: With publisher confirms (guaranteed delivery)
	example2WithConfirms(rabbitURI)

	time.Sleep(2 * time.Second)

	// Example 3: Batch publishing with confirms (high throughput)
	example3BatchWithConfirms(rabbitURI)

	log.Println("\n✅ All examples completed!")
}

// Example 1: Without confirms (faster but no guarantee)
func example1WithoutConfirms(rabbitURI string) {
	log.Println("📤 Example 1: WITHOUT Publisher Confirms")
	log.Println("=========================================")

	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithPublisherConfirms(false), // Disabled (default)
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "transactions.exchange", Type: "direct", Durable: true},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	start := time.Now()

	// Publish 10 transactions
	for i := 1; i <= 10; i++ {
		event := TransactionEvent{
			TxID:      fmt.Sprintf("TX-%03d", i),
			Amount:    float64(i) * 100,
			From:      fmt.Sprintf("ACC-%d", i),
			To:        fmt.Sprintf("ACC-%d", i+1),
			Timestamp: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, event)
		cancel()

		if err != nil {
			log.Printf("❌ Failed: %s", event.TxID)
		} else {
			log.Printf("📨 Published: %s", event.TxID)
		}
	}

	elapsed := time.Since(start)
	log.Printf("\n⏱️  Completed in: %v", elapsed)
	log.Printf("⚠️  Warning: No delivery guarantee - messages may be lost\n")
}

// Example 2: With confirms (guaranteed delivery)
func example2WithConfirms(rabbitURI string) {
	log.Println("📤 Example 2: WITH Publisher Confirms")
	log.Println("======================================")

	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithPublisherConfirms(true),           // Enabled
		config.WithConfirmTimeout(5*time.Second),     // Wait up to 5s for confirm
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "transactions.exchange", Type: "direct", Durable: true},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	start := time.Now()
	confirmed := 0
	failed := 0

	// Publish 10 transactions with confirmation
	for i := 1; i <= 10; i++ {
		event := TransactionEvent{
			TxID:      fmt.Sprintf("TX-%03d", i),
			Amount:    float64(i) * 100,
			From:      fmt.Sprintf("ACC-%d", i),
			To:        fmt.Sprintf("ACC-%d", i+1),
			Timestamp: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, event)
		cancel()

		if err != nil {
			log.Printf("❌ Failed: %s - %v", event.TxID, err)
			failed++
		} else {
			log.Printf("✅ Confirmed: %s", event.TxID)
			confirmed++
		}
	}

	elapsed := time.Since(start)
	log.Printf("\n⏱️  Completed in: %v", elapsed)
	log.Printf("✅ Confirmed: %d/%d", confirmed, confirmed+failed)
	log.Printf("🔒 Guarantee: All confirmed messages are persisted\n")
}

// Example 3: Batch with confirms (high throughput + guarantee)
func example3BatchWithConfirms(rabbitURI string) {
	log.Println("📤 Example 3: BATCH with Publisher Confirms")
	log.Println("============================================")

	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithPublisherConfirms(true),
		config.WithConfirmTimeout(10*time.Second),
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "transactions.exchange", Type: "direct", Durable: true},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	// Generate 100 transactions
	events := make([]rabbitmq.Event, 100)
	for i := 0; i < 100; i++ {
		events[i] = TransactionEvent{
			TxID:      fmt.Sprintf("TX-%04d", i+1),
			Amount:    float64(i+1) * 10,
			From:      fmt.Sprintf("ACC-%d", i),
			To:        fmt.Sprintf("ACC-%d", i+1),
			Timestamp: time.Now(),
		}
	}

	log.Println("Publishing 100 transactions with pipelining...")
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	result, err := eventBus.PublishBatch(ctx, events)
	cancel()

	elapsed := time.Since(start)

	if err != nil {
		log.Printf("❌ Fatal error: %v", err)
		return
	}

	log.Printf("\n📊 Batch Results:")
	log.Printf("   Total:     %d", result.Total)
	log.Printf("   Confirmed: %d", result.Success)
	log.Printf("   Failed:    %d", result.Failed)
	log.Printf("\n⏱️  Completed in: %v", elapsed)
	log.Printf("🚀 Throughput: %.0f msg/s", float64(result.Success)/elapsed.Seconds())
	log.Printf("🔒 All %d messages are guaranteed delivered\n", result.Success)

	if result.Failed > 0 {
		log.Println("\n❌ Failed Messages:")
		for _, batchErr := range result.Errors {
			log.Printf("   [%d] %v", batchErr.Index, batchErr.Error)
		}
	}
}
