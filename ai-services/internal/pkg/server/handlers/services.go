package handlers

import (
	"fmt"
	"net/http"

	"github.com/containers/podman/v5/pkg/domain/entities/types"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/project-ai-services/ai-services/internal/pkg/logger"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime"
	"github.com/project-ai-services/ai-services/internal/pkg/runtime/podman"
	"github.com/project-ai-services/ai-services/internal/pkg/server/models"
)

var log *zap.Logger

func init() {
	log = logger.GetLogger()
}

type servicesHandler struct {
	// container Runtime
	runtime runtime.Runtime
}

func NewServicesHandler() *servicesHandler {
	runtime, err := podman.NewPodmanClient()
	if err != nil {
		panic(fmt.Sprintf("failed connecting to container runtime: %s", err.Error()))
	}
	return &servicesHandler{runtime: runtime}
}

func (s *servicesHandler) Get(c *gin.Context) {
	resp, err := s.runtime.ListPods()
	if err != nil {
		log.Error("GET Services failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, models.ErrorResp{Error: models.Error{Code: http.StatusInternalServerError, Message: "Something went wrong. Please try again!"}})
	}

	var pods []*types.ListPodsReport
	if val, ok := resp.([]*types.ListPodsReport); ok {
		pods = val
	}

	convertToServiceObj := func(pods []*types.ListPodsReport) []models.Service {
		output := make([]models.Service, len(pods))
		for i, pod := range pods {
			output[i] = models.Service{ID: pod.Id, Name: pod.Name, Status: pod.Status}
		}
		return output
	}

	c.JSON(http.StatusOK, models.GetServicesResp{Services: convertToServiceObj(pods)})
}
