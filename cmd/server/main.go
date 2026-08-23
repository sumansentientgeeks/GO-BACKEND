package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "example/hello/docs" // Import swagger docs

	"example/hello/pkg/config"
	"example/hello/pkg/database"
	"example/hello/pkg/redis"

	"example/hello/internal/api/room"
	"example/hello/internal/api/user"
	"example/hello/internal/messaging"
	"example/hello/internal/repository"
	"example/hello/internal/service"
	"example/hello/internal/sfu"
	"example/hello/pkg/rabbitmq"
)


var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	UserID string
	RoomID string
	Conn   *websocket.Conn
}

type SignalMessage struct {
	Type     string          `json:"type"`
	RoomID   string          `json:"room_id,omitempty"`
	UserID   string          `json:"user_id,omitempty"`
	ToUserID string          `json:"to_user_id,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Text     string          `json:"text,omitempty"`
}

type Room struct {
	ID      string
	Clients map[string]*Client
	mu      sync.RWMutex
}

type Hub struct {
	Rooms map[string]*Room
	mu    sync.RWMutex
}

func newHub() *Hub {
	return &Hub{Rooms: make(map[string]*Room)}
}

func (h *Hub) GetOrCreateRoom(roomID string) *Room {
	h.mu.Lock()
	defer h.mu.Unlock()

	if room, ok := h.Rooms[roomID]; ok {
		return room
	}

	room := &Room{
		ID:      roomID,
		Clients: make(map[string]*Client),
	}
	h.Rooms[roomID] = room
	return room
}

func (h *Hub) JoinRoom(client *Client) {
	room := h.GetOrCreateRoom(client.RoomID)
	room.mu.Lock()
	room.Clients[client.UserID] = client
	room.mu.Unlock()
}

func (h *Hub) LeaveRoom(client *Client) {
	room, ok := h.Rooms[client.RoomID]
	if !ok {
		return
	}

	room.mu.Lock()
	delete(room.Clients, client.UserID)
	room.mu.Unlock()

	if len(room.Clients) == 0 {
		h.mu.Lock()
		delete(h.Rooms, room.ID)
		h.mu.Unlock()
	}
}

func (h *Hub) Broadcast(roomID string, msg SignalMessage) {
	room, ok := h.Rooms[roomID]
	if !ok {
		return
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, client := range room.Clients {
		if err := client.Conn.WriteJSON(msg); err != nil {
			log.Println("broadcast err:", err)
		}
	}
}

func (h *Hub) SendToUser(roomID, targetUserID string, msg SignalMessage) {
	room, ok := h.Rooms[roomID]
	if !ok {
		return
	}

	room.mu.RLock()
	defer room.mu.RUnlock()

	if client, ok := room.Clients[targetUserID]; ok {
		if err := client.Conn.WriteJSON(msg); err != nil {
			log.Println("send to user err:", err)
		}
	}
}

func newRouter() *gin.Engine {
	r := gin.Default()

	// Add CORS middleware
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
	r.Use(cors.New(corsConfig))

	// Initialize Redis Connection for Distributed Signaling & Presence
	if _, err := redis.Connect(); err != nil {
		log.Printf("Redis initialization warning (running in single-node mode): %v", err)
	}

	// Initialize WebRTC SFU Engine
	sfuManager, err := sfu.NewSFUManager()
	if err != nil {
		log.Fatalf("Failed to initialize SFU MediaEngine: %v", err)
	}


	hub := newHub()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "teams-sfu-backend"})
	})

	// Swagger Route
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	// User Routes
	db, err := database.Connect()
	if err != nil {
		log.Printf("Database connection warning (running without persistent DB): %v", err)
	} else {
		userRepo := repository.NewUserRepository(db)
		userService := service.NewUserService(userRepo)
		userHandler := user.NewUserHandler(userService)

		r.POST("/api/users/register", userHandler.Register)
		r.POST("/api/users/login", userHandler.Login)
	}

	livekitService := service.NewLiveKitService()
	callHandler := room.NewCallHandler(livekitService)

	// RPC Client (Requestor in EIP Request-Reply Pattern)
	var rpcClient *messaging.RPCClient
	rmq, err := rabbitmq.Connect()
	if err != nil {
		log.Printf("RabbitMQ connection warning (running without message broker): %v", err)
	} else {
		client, err := messaging.NewRPCClient(rmq)
		if err != nil {
			log.Printf("Failed to initialize RPC Client: %v", err)
		} else {
			rpcClient = client
			log.Println("RPC Client successfully initialized for Request-Reply communication")
		}
	}

	// RPC Trigger Route
	r.POST("/api/rpc/call", func(c *gin.Context) {
		if rpcClient == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "RPC worker communication unavailable (RabbitMQ broker not connected)",
			})
			return
		}

		var payload struct {
			Action string `json:"action" binding:"required"`
			Params any    `json:"params,omitempty"`
		}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		var result any
		if err := rpcClient.Call(c.Request.Context(), payload.Action, payload.Params, &result); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "ok",
			"action": payload.Action,
			"result": result,
		})
	})

	// Room API
	r.GET("/api/rooms/:id/call-token", callHandler.GetJoinToken)

	r.POST("/api/rooms/create", func(c *gin.Context) {
		var payload struct {
			RoomID string `json:"room_id"`
		}
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if payload.RoomID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "room_id is required"})
			return
		}

		sfuManager.GetOrCreateRoom(payload.RoomID)
		hub.GetOrCreateRoom(payload.RoomID)
		c.JSON(http.StatusOK, gin.H{"status": "ok", "room_id": payload.RoomID})
	})

	// SFU WebRTC Signaling WebSocket Endpoint
	r.GET("/ws/sfu", sfu.HandleSFUWebSocket(sfuManager))

	// Direct Signaling Endpoint (fallback / legacy peer-to-peer)
	r.GET("/ws", func(c *gin.Context) {
		roomID := c.Query("room_id")
		userID := c.Query("user_id")
		if roomID == "" || userID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "room_id and user_id are required"})
			return
		}

		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Println("upgrade err:", err)
			return
		}

		client := &Client{UserID: userID, RoomID: roomID, Conn: conn}
		hub.JoinRoom(client)
		defer func() {
			hub.LeaveRoom(client)
			_ = conn.Close()
		}()

		for {
			var msg SignalMessage
			if err := conn.ReadJSON(&msg); err != nil {
				log.Println("read err:", err)
				return
			}

			switch msg.Type {
			case "join_room":
				hub.JoinRoom(client)
			case "chat_message":
				msg.RoomID = roomID
				msg.UserID = userID
				hub.Broadcast(roomID, msg)
			case "offer", "answer", "ice_candidate":
				msg.RoomID = roomID
				msg.UserID = userID
				if msg.ToUserID != "" {
					hub.SendToUser(roomID, msg.ToUserID, msg)
				} else {
					hub.Broadcast(roomID, msg)
				}
			case "leave_room":
				hub.LeaveRoom(client)
				return
			default:
				log.Println("unknown message type:", msg.Type)
			}
		}
	})

	return r
}

// @title Hello API
// @version 1.0
// @description This is a sample WebRTC and chat server.
// @host localhost:8080
// @BasePath /
func main() {
	config.Load()
	r := newRouter()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	log.Printf("Starting Teams WebRTC SFU Server on %s...", port)
	if err := r.Run(port); err != nil {
		log.Fatal(err)
	}
}
