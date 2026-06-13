package main

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"

	"food-delivery/notification-service/kafka"
)

func main() {
	kafka.ConsumeNotifications()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8084"
	}

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":    "ok",
			"consumers": []string{"order.confirmed", "payment.done"},
		})
	})

	r.Run(":" + port)
}
