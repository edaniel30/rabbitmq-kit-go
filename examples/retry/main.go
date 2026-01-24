// Automatic Retry Mechanism with rabbitmq-kit-go
//
// This example demonstrates:
// - Configuring automatic retry with MaxRetries
// - Exponential backoff behavior (built-in via x-retry-count header)
// - Tracking retry attempts in message headers
// - Integration with DLQ after max retries exceeded
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
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	rabbitmq "github.com/edaniel30/rabbitmq-kit-go"
	"github.com/edaniel30/rabbitmq-kit-go/config"
	"github.com/edaniel30/rabbitmq-kit-go/router"
)

// EmailEvent represents an email sending event
type EmailEvent struct {
	EmailID   string    `json:"email_id"`
	To        string    `json:"to"`
	Subject   string    `json:"subject"`
	Body      string    `json:"body"`
	Timestamp time.Time `json:"timestamp"`
}

func (e EmailEvent) Type() string     { return "email.send" }
func (e EmailEvent) Exchange() string { return "emails.exchange" }
func (e EmailEvent) ToMap() map[string]any {
	return map[string]any{
		"type":      e.Type(),
		"email_id":  e.EmailID,
		"to":        e.To,
		"subject":   e.Subject,
		"body":      e.Body,
		"timestamp": e.Timestamp,
	}
}

func main() {
	rabbitURI := os.Getenv("RABBITMQ_URI")
	if rabbitURI == "" {
		rabbitURI = "amqp://guest:guest@localhost:5672/"
	}

	// Create EventBus with retry configuration
	eventBus, err := rabbitmq.NewEventBus(
		config.DefaultConfig(),
		config.WithURI(rabbitURI),
		config.WithMaxRetries(3), // Retry up to 3 times before giving up
		config.WithDLQ(true),     // Send to DLQ after max retries
		config.WithExchanges([]config.ExchangeConfig{
			{Name: "emails.exchange", Type: "direct", Durable: true},
		}),
		config.WithQueues([]config.QueueConfig{
			{
				Name:        "emails.queue",
				Exchange:    "emails.exchange",
				RoutingKeys: []string{"email.send"},
				Durable:     true,
			},
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create EventBus: %v", err)
	}
	defer func() { _ = eventBus.Close() }()

	log.Println("✅ Connected to RabbitMQ with Retry Mechanism")
	log.Println("   Max Retries: 3")
	log.Println("   DLQ Enabled: Yes")
	log.Println("")

	// Register handler that simulates transient failures
	eventBus.RegisterHandler("email.send", &EmailHandler{})

	// Register DLQ handler for permanently failed emails
	eventBus.RegisterDLQHandler("email.send", &EmailDLQHandler{})

	// Start main consumer
	go func() {
		log.Println("📥 Starting email consumer...")
		if err := eventBus.StartConsume("emails.queue", 2); err != nil {
			log.Fatalf("Failed to start consumer: %v", err)
		}
	}()

	// Start DLQ consumer
	go func() {
		time.Sleep(1 * time.Second)
		log.Println("📥 Starting DLQ consumer...")
		if err := eventBus.StartConsumeDLQ("dlq.emails.queue", 1); err != nil {
			log.Fatalf("Failed to start DLQ consumer: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	// Publish test emails
	log.Println("\n📤 Publishing test emails...")
	publishTestEmails(eventBus)

	// Wait for processing
	log.Println("\n⏳ Processing emails (watch retry attempts)...")
	time.Sleep(30 * time.Second)

	// Wait for signal
	log.Println("\n⏳ Press Ctrl+C to shutdown...")
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("\n🛑 Shutting down...")
}

// EmailHandler simulates sending emails with transient failures
type EmailHandler struct{}

func (h *EmailHandler) Execute(ctx *router.MessageContext) error {
	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	emailID := data["email_id"].(string)
	to := data["to"].(string)
	subject := data["subject"].(string)

	// Get retry count from message context
	retryCount := ctx.GetRetryCount()

	log.Printf("\n📧 Processing email: %s", emailID)
	log.Printf("   To: %s", to)
	log.Printf("   Subject: %s", subject)
	log.Printf("   Retry Attempt: %d/3", retryCount)

	// Simulate transient failures (70% success rate on first try)
	// Success rate increases with retries
	successProbability := 0.3 + (float64(retryCount) * 0.3)
	if rand.Float64() < successProbability {
		log.Printf("✅ Email sent successfully: %s", emailID)
		return nil
	}

	// Simulate different types of failures
	failureTypes := []string{
		"SMTP connection timeout",
		"Temporary server error",
		"Rate limit exceeded",
		"Network unreachable",
	}
	failureMsg := failureTypes[rand.Intn(len(failureTypes))]

	log.Printf("❌ Email failed: %s - %s (will retry)", emailID, failureMsg)
	return fmt.Errorf("email send failed: %s", failureMsg)
}

// EmailDLQHandler handles emails that exceeded max retries
type EmailDLQHandler struct{}

func (h *EmailDLQHandler) Execute(ctx *router.MessageContext) error {
	dlqMsg := router.NewDLQMessage(ctx)

	var data map[string]any
	if err := ctx.BindJSON(&data); err != nil {
		return err
	}

	emailID := data["email_id"].(string)
	to := data["to"].(string)

	log.Println("\n⛔ PERMANENTLY FAILED EMAIL")
	log.Println("===========================")
	log.Printf("Email ID: %s", emailID)
	log.Printf("To: %s", to)
	log.Printf("Retry Count: %d", dlqMsg.RetryCount)
	log.Printf("Death Reason: %s", dlqMsg.DeathReason)
	log.Printf("Original Queue: %s", dlqMsg.OriginalQueue)
	log.Println("")
	log.Println("Action: Sending alert to monitoring system")
	log.Println("Action: Logging to dead letter storage")
	log.Println("")

	// In production, you might:
	// 1. Send alert/notification
	// 2. Store in database for manual review
	// 3. Trigger compensation logic
	// 4. Update metrics/monitoring

	return ctx.Ack()
}

func publishTestEmails(eventBus *rabbitmq.EventBus) {
	emails := []EmailEvent{
		{
			EmailID:   "EMAIL-001",
			To:        "user1@example.com",
			Subject:   "Welcome to our service!",
			Body:      "Thank you for signing up...",
			Timestamp: time.Now(),
		},
		{
			EmailID:   "EMAIL-002",
			To:        "user2@example.com",
			Subject:   "Password reset request",
			Body:      "Click here to reset your password...",
			Timestamp: time.Now(),
		},
		{
			EmailID:   "EMAIL-003",
			To:        "user3@example.com",
			Subject:   "Your order confirmation",
			Body:      "Your order #12345 has been confirmed...",
			Timestamp: time.Now(),
		},
		{
			EmailID:   "EMAIL-004",
			To:        "user4@example.com",
			Subject:   "Weekly newsletter",
			Body:      "Here's what's new this week...",
			Timestamp: time.Now(),
		},
		{
			EmailID:   "EMAIL-005",
			To:        "user5@example.com",
			Subject:   "Account verification",
			Body:      "Please verify your email address...",
			Timestamp: time.Now(),
		},
	}

	for _, email := range emails {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := eventBus.Publish(ctx, email)
		cancel()

		if err != nil {
			log.Printf("❌ Failed to publish email: %v", err)
		} else {
			log.Printf("📨 Published: %s", email.EmailID)
		}

		time.Sleep(500 * time.Millisecond)
	}
}
