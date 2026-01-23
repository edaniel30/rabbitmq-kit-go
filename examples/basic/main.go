// Basic Publisher and Consumer with rabbitmq-kit-go
//
// This example demonstrates:
// - Creating an EventBus with configuration
// - Publishing events to RabbitMQ
// - Consuming events with handlers
// - Graceful shutdown
//
// Prerequisites:
// - RabbitMQ running on localhost:5672
// - Or set RABBITMQ_URI environment variable
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

// OrderCreatedEvent represents an order creation event
type OrderCreatedEvent struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

// Implement Event interface
func (e OrderCreatedEvent) Type() string {
	return "order.created"
}

func (e OrderCreatedEvent) Exchange() string {
	return "orders.exchange"
}

func (e OrderCreatedEvent) ToMap() map[string]any {
	return map[string]any{
		"type":      e.Type(),
		"order_id":  e.OrderID,
		"user_id":   e.UserID,
		"amount":    e.Amount,
		"timestamp": e.Timestamp,
	}
}

func main() {
	// Get RabbitMQ URI from environment or use default
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	// Create EventBus with configuration
	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithExchanges([]config.ExchangeConfig{
			{
				Name:    "orders.exchange",
				Type:    "direct",
				Durable: true,
			},
		}),
		config.WithQueues([]config.QueueConfig{
			{
				Name:        "orders.queue",
				Exchange:    "orders.exchange",
				RoutingKeys: []string{"order.created"},
				Durable:     true,
			},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	log.Println("✅ Connected to RabbitMQ")

	// Register event handler
	eventBus.RegisterHandler("order.created", &OrderCreatedHandler{})

	// Start consuming messages in a goroutine
	go func() {
		log.Println("📥 Starting consumer with 3 workers...")
		if err := eventBus.StartConsume("orders.queue", 3); err != nil {
			log.Fatalf("Failed to start consumer: %v", err)
		}
	}()

	// Give consumer time to start
	time.Sleep(1 * time.Second)

	// Publish some events
	log.Println("📤 Publishing events...")
	for i := 1; i <= 5; i++ {
		event := OrderCreatedEvent{
			OrderID:   fmt.Sprintf("ORD-%d", i),
			UserID:    fmt.Sprintf("USER-%d", i),
			Amount:    99.99 * float64(i),
			Timestamp: time.Now(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, event)
		cancel()

		if err != nil {
			log.Printf("❌ Failed to publish event: %v", err)
		} else {
			log.Printf("✅ Published: %s (%.2f)", event.OrderID, event.Amount)
		}

		time.Sleep(500 * time.Millisecond)
	}

	// Wait for signal to shutdown
	log.Println("\n⏳ Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
}

// OrderCreatedHandler handles order.created events
type OrderCreatedHandler struct{}

func (h *OrderCreatedHandler) Execute(ctx *router.MessageContext) error {
	// Parse the event
	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		log.Printf("❌ Failed to parse message: %v", err)
		return err
	}

	orderID := data["order_id"].(string)
	amount := data["amount"].(float64)

	log.Printf("🎉 Processed order: %s - $%.2f", orderID, amount)

	// Simulate processing time
	time.Sleep(100 * time.Millisecond)

	return nil
}
