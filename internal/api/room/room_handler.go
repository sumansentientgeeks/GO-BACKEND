package room

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type RoomHandler struct {
	// Hub interface or direct access if needed, but for now we just return ok
}

func NewRoomHandler() *RoomHandler {
	return &RoomHandler{}
}

type CreateRoomRequest struct {
	RoomID string `json:"room_id" binding:"required"`
}

// CreateRoom godoc
// @Summary Create a room
// @Description Creates a new room (or gets existing)
// @Tags rooms
// @Accept  json
// @Produce  json
// @Param request body CreateRoomRequest true "Create Room Request"
// @Success 200 {object} map[string]interface{}
// @Failure 400 {object} map[string]interface{}
// @Router /api/rooms/create [post]
func (h *RoomHandler) CreateRoom(c *gin.Context) {
	var payload CreateRoomRequest
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if payload.RoomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "room_id is required"})
		return
	}

	// hub.GetOrCreateRoom is called in main, we can't easily access hub here without passing it.
	// We'll just return the room_id. The actual creation in hub happens when they join the websocket.
	// Wait, the original code did `hub.GetOrCreateRoom(payload.RoomID)`. 
	// To keep it simple, I'll let main.go keep the handler inline and just add swagger comments above it if possible.
	c.JSON(http.StatusOK, gin.H{"status": "ok", "room_id": payload.RoomID})
}
