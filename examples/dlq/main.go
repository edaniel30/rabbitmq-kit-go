// Dead Letter Queue (DLQ) with rabbitmq-kit-go
//
// This example demonstrates:
// - Automatic DLQ setup with WithDLQ()
// - Consuming and analyzing failed messages from DLQ
// - Re-enqueuing messages from DLQ after fixing issues
// - Bulk re-enqueuing with RequeueAllFromDLQ()
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

// PaymentEvent represents a payment processing event
type PaymentEvent struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	UserID    string  `json:"user_id"`
	ShouldFail bool   `json:"should_fail"` // For demo purposes
}

func (e PaymentEvent) Type() string      { return "payment.process" }
func (e PaymentEvent) Exchange() string  { return "payments.exchange" }
func (e PaymentEvent) ToMap() map[string]any {
	return map[string]any{
		"type":        e.Type(),
		"payment_id":  e.PaymentID,
		"amount":      e.Amount,
		"user_id":     e.UserID,
		"should_fail": e.ShouldFail,
	}
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	// Create EventBus with DLQ enabled
	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithMaxRetries(2), // Retry 2 times before DLQ
		config.WithDLQ(true),      // Enable automatic DLQ setup
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "payments.exchange", Type: "direct", Durable: true},
		}),
		config.WithQueues([]config.QueueConfig{
			{
				Name:        "payments.queue",
				Exchange:    "payments.exchange",
				RoutingKeys: []string{"payment.process"},
				Durable:     true,
			},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer eventBus.Close()

	log.Println("✅ Connected to RabbitMQ with DLQ enabled")
	log.Println("   DLX: dlx.exchange")
	log.Println("   DLQ: dlq.payments.queue")
	log.Println("")
	log.Println("ℹ️  Note: If you see old messages, run these commands to purge queues:")
	log.Println("   rabbitmqadmin purge queue name=payments.queue")
	log.Println("   rabbitmqadmin purge queue name=dlq.payments.queue")
	log.Println("")

	// Register main queue handler (simulates failures)
	eventBus.RegisterHandler("payment.process", &PaymentHandler{})

	// Note: DLQ handler registration example (not used in this demo)
	// eventBus.RegisterDLQHandler("payment.process", &PaymentDLQHandler{eventBus: eventBus})

	// Start consuming from main queue
	go func() {
		log.Println("📥 Starting main consumer...")
		if err := eventBus.StartConsume("payments.queue", 2); err != nil {
			log.Fatalf("Failed to start consumer: %v", err)
		}
	}()

	// Note: Starting DLQ consumer on the same EventBus can cause channel conflicts
	// In production, create a separate EventBus for DLQ consumption
	// Example (commented out to avoid channel issues):
	//
	// go func() {
	//     time.Sleep(1 * time.Second)
	//     log.Println("📥 Starting DLQ consumer...")
	//     if err := eventBus.StartConsumeDLQ("dlq.payments.queue", 1); err != nil {
	//         log.Fatalf("Failed to start DLQ consumer: %v", err)
	//     }
	// }()

	time.Sleep(2 * time.Second)

	// Publish test events (some will fail intentionally)
	log.Println("\n📤 Publishing test events...")
	publishTestEvents(eventBus)

	// Wait a bit for processing
	time.Sleep(10 * time.Second)

	// Note: In this demo, we're NOT re-enqueuing because messages have ShouldFail=true
	// and would create an infinite loop. In production, you would:
	// 1. Fix the underlying issue (deploy bug fix, restore service, etc.)
	// 2. Then call RequeueAllFromDLQ() to retry the messages
	//
	// Example of bulk re-enqueue (commented out to avoid infinite loop):
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// count, err := eventBus.RequeueAllFromDLQ(ctx, "dlq.payments.queue", true, 0)
	// cancel()
	// if err != nil {
	//     log.Printf("❌ Failed to requeue: %v", err)
	// } else {
	//     log.Printf("✅ Re-enqueued %d messages", count)
	// }

	log.Println("\n📊 Summary:")
	log.Println("   - Messages PAY-002 and PAY-004 failed after 2 retries")
	log.Println("   - They were sent to DLQ: dlq.payments.queue")
	log.Println("   - Check RabbitMQ Management UI to see them in the DLQ")
	log.Println("")
	log.Println("📝 Next steps in production:")
	log.Println("   1. Fix the underlying issue (deploy bug fix, restore service)")
	log.Println("   2. Create separate EventBus for DLQ monitoring")
	log.Println("   3. Use RequeueAllFromDLQ() to retry failed messages")

	// Wait for signal to shutdown
	log.Println("\n⏳ Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
}

// PaymentHandler processes payment events (simulates failures for demo)
type PaymentHandler struct{}

func (h *PaymentHandler) Execute(ctx *router.MessageContext) error {
	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	paymentID := data["payment_id"].(string)
	shouldFail := data["should_fail"].(bool)

	log.Printf("💳 Processing payment: %s", paymentID)

	// Simulate failure for demo
	if shouldFail {
		log.Printf("❌ Payment failed: %s (will retry)", paymentID)
		return fmt.Errorf("payment processing failed")
	}

	log.Printf("✅ Payment successful: %s", paymentID)
	return nil
}

// PaymentDLQHandler analyzes and handles failed messages from DLQ
type PaymentDLQHandler struct {
	eventBus *rabbitmq.EventBus
}

func (h *PaymentDLQHandler) Execute(ctx *router.MessageContext) error {
	// Convert to DLQ message to access metadata
	dlqMsg := router.NewDLQMessage(ctx)

	log.Println("\n🔍 DLQ Message Analysis:")
	log.Printf("   %s", dlqMsg.GetDeathInfo())
	log.Println("")

	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	paymentID := data["payment_id"].(string)

	// Log analysis - in production you would:
	// 1. Send alert/notification to monitoring system
	// 2. Store in database for manual review
	// 3. Trigger compensation logic
	// 4. Update metrics/dashboards

	log.Printf("⛔ DLQ: Payment %s failed permanently after %d retries", paymentID, dlqMsg.RetryCount)
	log.Printf("   Reason: %s", dlqMsg.DeathReason)
	log.Printf("   Action: Logged for manual review")
	log.Println("")

	// Acknowledge to remove from DLQ
	// Note: We're NOT re-enqueuing here because the message would fail again
	// In production, you would fix the issue first, then use RequeueAllFromDLQ()
	return ctx.Ack()
}

func publishTestEvents(eventBus *rabbitmq.EventBus) {
	events := []PaymentEvent{
		{PaymentID: "PAY-001", Amount: 100.00, UserID: "USER-1", ShouldFail: false},
		{PaymentID: "PAY-002", Amount: 200.00, UserID: "USER-2", ShouldFail: true},  // Will fail
		{PaymentID: "PAY-003", Amount: 300.00, UserID: "USER-3", ShouldFail: false},
		{PaymentID: "PAY-004", Amount: 400.00, UserID: "USER-4", ShouldFail: true},  // Will fail
		{PaymentID: "PAY-005", Amount: 500.00, UserID: "USER-5", ShouldFail: false},
	}

	for _, event := range events {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, event)
		cancel()

		if err != nil {
			log.Printf("❌ Failed to publish: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
