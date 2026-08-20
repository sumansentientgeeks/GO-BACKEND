package room

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"

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
// @Param user_id query string false "User ID"
// @Param user_name query string false "User Name"
// @Param Authorization header string false "Bearer {token}"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Failure 500 {object} map[string]interface{}
// @Router /api/rooms/{id}/call-token [get]
func (h *CallHandler) GetJoinToken(c *gin.Context) {
	roomID := c.Param("id")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room id is required"})
		return
	}

	// 1. Check if user_id was set by auth middleware
	userID, exists := c.Get("user_id")
	if !exists || userID == "" {
		// 2. Check Authorization header manually if provided
		authHeader := c.GetHeader("Authorization")
		if authHeader != "" {
			parts := strings.Split(authHeader, " ")
			if len(parts) == 2 && parts[0] == "Bearer" {
				secret := os.Getenv("JWT_SECRET")
				if secret == "" {
					secret = "super_secret_default_key"
				}
				token, err := jwt.Parse(parts[1], func(token *jwt.Token) (interface{}, error) {
					return []byte(secret), nil
				})
				if err == nil && token.Valid {
					if claims, ok := token.Claims.(jwt.MapClaims); ok {
						userID = claims["user_id"]
						exists = true
					}
				}
			}
		}
	}

	// 3. Fallback to query param or generate guest ID
	if !exists || userID == "" {
		userID = c.Query("user_id")
		if userID == "" {
			userID = fmt.Sprintf("guest_%d", time.Now().UnixMilli())
		}
	}

	var identity string
	switch v := userID.(type) {
	case string:
		identity = v
	case float64:
		identity = fmt.Sprintf("%.0f", v)
	default:
		identity = fmt.Sprintf("%v", v)
	}

	userName := c.Query("user_name")
	if userName == "" {
		userName = "User " + identity
	}

	token, err := h.livekitService.GenerateToken(roomID, identity, userName)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to generate token: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"token": token,
	})
}
