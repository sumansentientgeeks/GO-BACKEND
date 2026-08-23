package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"example/hello/internal/messaging"
	"example/hello/pkg/config"
	"example/hello/pkg/rabbitmq"
)

func main() {
	config.Load()
	log.Println("==================================================")
	log.Println("  Starting RabbitMQ RPC Worker (Replier Node)    ")
	log.Println("  Pattern: Enterprise Integration Patterns (EIP) ")
	log.Println("           Request-Reply Messaging Pattern       ")
	log.Println("==================================================")

	// 1. Establish RabbitMQ connection
	rmq, err := rabbitmq.Connect()
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ broker: %v", err)
	}
	defer func() {
		_ = rmq.Close()
	}()

	// 2. Instantiate RPC Worker
	worker, err := messaging.NewRPCWorker(rmq,
		messaging.WithWorkerQueue(messaging.DefaultRPCQueue),
		messaging.WithPrefetch(1), // Fair dispatch QoS
	)
	if err != nil {
		log.Fatalf("Failed to initialize RPC Worker: %v", err)
	}

	// 3. Register custom application handlers
	worker.RegisterHandler("user_validate", func(ctx context.Context, req *messaging.RPCRequest) (*messaging.RPCResponse, error) {
		var input struct {
			Email string `json:"email"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &input)
		}
		if input.Email == "" {
			return messaging.NewErrorResponse("email is required"), nil
		}

		return messaging.NewSuccessResponse(map[string]any{
			"email":     input.Email,
			"valid":     true,
			"verified":  true,
			"checkedAt": time.Now().UTC().Format(time.RFC3339),
		})
	})

	worker.RegisterHandler("room_metrics", func(ctx context.Context, req *messaging.RPCRequest) (*messaging.RPCResponse, error) {
		var input struct {
			RoomID string `json:"room_id"`
		}
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &input)
		}

		return messaging.NewSuccessResponse(map[string]any{
			"room_id":       input.RoomID,
			"active_tracks": 4,
			"bitrate_kbps":  2400,
			"status":        "healthy",
			"timestamp":     time.Now().UnixMilli(),
		})
	})

	// 4. Start RPC Worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.Start(ctx); err != nil {
		log.Fatalf("Failed to start RPC Worker: %v", err)
	}

	log.Printf("[RPC Worker] Running and waiting for RPC requests on [%s]...", messaging.DefaultRPCQueue)

	// 5. Graceful shutdown handler
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[RPC Worker] Shutting down gracefully...")
	cancel()
	worker.Stop()
	log.Println("[RPC Worker] Worker stopped cleanly.")
}
