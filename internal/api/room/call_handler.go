package room

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"


	"example/hello/internal/service"
)

type CallHandler struct {
	livekitService *service.LiveKitService
}

func NewCallHandler(livekitService *service.LiveKitService) *CallHandler {
	return &CallHandler{
		livekitService: livekitService,
	}
}

// GetJoinToken godoc
// @Summary Get LiveKit Join Token
// @Description Generates a LiveKit access token to join a WebRTC room
// @Tags rooms
// @Produce  json
// @Param id path string true "Room ID"
// @Param Authorization header string true "Bearer {token}"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Failure 401 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/rooms/{id}/call-token [get]
func (h *CallHandler) GetJoinToken(c *gin.Context) {
	roomID := c.Param("id")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room id is required"})
		return
	}

	// Assuming auth_middleware sets "user_id" in context
	userID, exists := c.Get("user_id")
	if !exists {
		// Fallback to query param if auth middleware is not used for this endpoint yet
		userID = c.Query("user_id")
		if userID == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "user id is required"})
			return
		}
	}

	var identity string
	switch v := userID.(type) {
	case string:
		identity = v
	case float64:
		// jwt.MapClaims unmarshals numbers as float64
		identity = fmt.Sprintf("%.0f", v)
	default:
		identity = fmt.Sprintf("%v", v)
	}

	participantName := "User " + identity // Or fetch from DB

	token, err := h.livekitService.GenerateToken(roomID, identity, participantName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token": token,
	})
}
