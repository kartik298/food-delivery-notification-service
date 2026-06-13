package main

import (
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"food-delivery/notification-service/kafka"
)

func main() {
	kafka.ConsumeAll()

	r := gin.Default()

	r.GET("/notifications/user/:user_id", func(c *gin.Context) {
		uid, _ := strconv.ParseUint(c.Param("user_id"), 10, 64)
		notifs := kafka.GetUserNotifications(uint(uid))
		if notifs == nil {
			notifs = []kafka.Notification{}
		}
		c.JSON(http.StatusOK, gin.H{"notifications": notifs, "count": len(notifs)})
	})

	r.POST("/notifications/send", func(c *gin.Context) {
		var req kafka.Notification
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"sent": true})
	})

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "ok",
			"service": "notification-service",
			"topics": []string{
				"order.placed", "order.confirmed", "order.preparing",
				"delivery.assigned", "order.out_for_delivery", "order.delivered",
				"payment.initiated", "payment.done", "payment.failed", "payment.refunded",
			},
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}
	r.Run(":" + port)
}
