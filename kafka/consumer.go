package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Notification struct {
	ID        string    `json:"id"`
	UserID    uint      `json:"user_id"`
	Type      string    `json:"type"`
	Title     string    `json:"title"`
	Body      string    `json:"body"`
	OrderID   uint      `json:"order_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	Read      bool      `json:"read"`
}

var (
	NotifStore = make(map[uint][]Notification)
	storeMu    sync.RWMutex
)

func storeNotif(n Notification) {
	storeMu.Lock()
	defer storeMu.Unlock()
	NotifStore[n.UserID] = append(NotifStore[n.UserID], n)
}

func GetUserNotifications(userID uint) []Notification {
	storeMu.RLock()
	defer storeMu.RUnlock()
	return NotifStore[userID]
}

func brokers() string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		return "localhost:9092"
	}
	return b
}

func sendEmail(userID uint, title, body string) {
	log.Printf("[EMAIL] user=%d | %s | %s", userID, title, body)
}

func sendPush(userID uint, title, body string) {
	log.Printf("[PUSH]  user=%d | %s | %s", userID, title, body)
}

func sendSMS(userID uint, body string) {
	log.Printf("[SMS]   user=%d | %s", userID, body)
}

func notify(userID uint, orderID uint, notifType, title, body string) {
	n := Notification{
		ID:        fmt.Sprintf("%d-%s-%d", userID, notifType, time.Now().UnixNano()),
		UserID:    userID,
		Type:      notifType,
		Title:     title,
		Body:      body,
		OrderID:   orderID,
		CreatedAt: time.Now(),
	}
	storeNotif(n)
	sendEmail(userID, title, body)
	sendPush(userID, title, body)
}

func consume(topic, groupID string, handler func(map[string]interface{})) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{brokers()},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.LastOffset,
	})
	defer r.Close()
	log.Printf("[KAFKA] consuming: %s", topic)
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[KAFKA] read error on %s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}
		var data map[string]interface{}
		if err := json.Unmarshal(msg.Value, &data); err != nil {
			log.Printf("[KAFKA] unmarshal error: %v", err)
			continue
		}
		handler(data)
	}
}

func uint64Val(m map[string]interface{}, key string) uint {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return uint(n)
		case int:
			return uint(n)
		}
	}
	return 0
}

func strVal(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprintf("%v", v)
	}
	return ""
}

func ConsumeAll() {
	topics := map[string]func(map[string]interface{}){
		"order.placed": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			notify(uid, oid, "order.placed", "Order Received!", fmt.Sprintf("Your order #%d has been placed. We're confirming with the restaurant.", oid))
		},
		"order.confirmed": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			notify(uid, oid, "order.confirmed", "Order Confirmed!", fmt.Sprintf("Great news! Restaurant confirmed order #%d and will start preparing soon.", oid))
		},
		"order.preparing": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			notify(uid, oid, "order.preparing", "Food Being Prepared", fmt.Sprintf("The chef is now preparing your order #%d. Sit tight!", oid))
		},
		"delivery.assigned": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			riderName := strVal(d, "rider_name")
			otp := strVal(d, "otp")
			eta := uint64Val(d, "estimated_minutes")
			notify(uid, oid, "delivery.assigned",
				"Rider Assigned!",
				fmt.Sprintf("Rider %s is coming to pick up your order #%d. OTP: %s | ETA: %d mins", riderName, oid, otp, eta))
		},
		"order.out_for_delivery": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			notify(uid, oid, "order.out_for_delivery", "Order On The Way!", fmt.Sprintf("Your order #%d is out for delivery! Rider is heading to you.", oid))
		},
		"order.delivered": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			notify(uid, oid, "order.delivered", "Order Delivered!", fmt.Sprintf("Enjoy your meal! Please rate your experience for order #%d.", oid))
		},
		"payment.initiated": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			amount := strVal(d, "amount")
			method := strVal(d, "method")
			notify(uid, oid, "payment.initiated", "Payment Initiated", fmt.Sprintf("Payment of Rs.%s via %s for order #%d is being processed.", amount, method, oid))
		},
		"payment.done": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			amount := strVal(d, "amount")
			ref := strVal(d, "transaction_ref")
			notify(uid, oid, "payment.done", "Payment Successful!", fmt.Sprintf("Rs.%s paid for order #%d. Ref: %s", amount, oid, ref))
			sendSMS(uid, fmt.Sprintf("Your payment of Rs.%s was successful. Ref: %s", amount, ref))
		},
		"payment.failed": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			reason := strVal(d, "reason")
			notify(uid, oid, "payment.failed", "Payment Failed", fmt.Sprintf("Payment for order #%d failed: %s. Please retry.", oid, reason))
		},
		"payment.refunded": func(d map[string]interface{}) {
			uid := uint64Val(d, "user_id")
			oid := uint64Val(d, "order_id")
			amount := strVal(d, "amount")
			notify(uid, oid, "payment.refunded", "Refund Processed", fmt.Sprintf("Refund of Rs.%s for order #%d has been processed.", amount, oid))
		},
	}
	for topic, handler := range topics {
		go consume(topic, "notification-service", handler)
	}
}
