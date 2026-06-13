package kafka

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

type NotificationPayload struct {
	OrderID uint    `json:"order_id"`
	UserID  uint    `json:"user_id"`
	Event   string  `json:"event"`
	Amount  float64 `json:"amount,omitempty"`
}

func sendEmail(payload NotificationPayload) {
	log.Printf("[EMAIL] To user %d: Order %d - %s", payload.UserID, payload.OrderID, payload.Event)
}

func sendPush(payload NotificationPayload) {
	log.Printf("[PUSH] To user %d: Order %d - %s", payload.UserID, payload.OrderID, payload.Event)
}

func ConsumeNotifications() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	topics := []string{"order.confirmed", "payment.done"}
	for _, topic := range topics {
		go func(t string) {
			r := kafka.NewReader(kafka.ReaderConfig{
				Brokers: []string{brokers},
				Topic:   t,
				GroupID: "notification-service",
			})
			defer r.Close()
			log.Printf("Notification service consuming topic: %s", t)
			for {
				msg, err := r.ReadMessage(context.Background())
				if err != nil {
					log.Printf("error reading from %s: %v", t, err)
					continue
				}
				var p NotificationPayload
				if err := json.Unmarshal(msg.Value, &p); err != nil {
					log.Printf("error unmarshaling: %v", err)
					continue
				}
				p.Event = t
				sendEmail(p)
				sendPush(p)
			}
		}(topic)
	}
}
