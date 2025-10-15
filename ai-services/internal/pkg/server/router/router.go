package router

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/project-ai-services/ai-services/internal/pkg/server/handlers"
)

func CreateRouter() *gin.Engine {
	router := gin.Default()
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	v1 := router.Group("/api/v1")

	serviceHandler := handlers.NewServicesHandler()
	v1.GET("/services", serviceHandler.Get)

	return router
}
