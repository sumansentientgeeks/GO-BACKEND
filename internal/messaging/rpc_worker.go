package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"example/hello/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RPCWorker implements the Replier component in the EIP Request-Reply pattern.
// It consumes request messages from a point-to-point queue, processes them,
// and routes the reply message back to the Return Address (ReplyTo) tagged with the CorrelationID.
type RPCWorker struct {
	rmq           *rabbitmq.RabbitMQ
	queueName     string
	prefetchCount int
	handlers      map[string]RPCHandler
	mu            sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
}

// WorkerOption configures the RPCWorker
type WorkerOption func(*RPCWorker)

// WithWorkerQueue sets a custom request queue name
func WithWorkerQueue(queue string) WorkerOption {
	return func(w *RPCWorker) {
		if queue != "" {
			w.queueName = queue
		}
	}
}

// WithPrefetch sets the prefetch QoS count (fair dispatch)
func WithPrefetch(count int) WorkerOption {
	return func(w *RPCWorker) {
		if count > 0 {
			w.prefetchCount = count
		}
	}
}

// NewRPCWorker creates a new RPC Worker instance
func NewRPCWorker(rmq *rabbitmq.RabbitMQ, opts ...WorkerOption) (*RPCWorker, error) {
	if rmq == nil {
		return nil, fmt.Errorf("rabbitmq client is required")
	}

	worker := &RPCWorker{
		rmq:           rmq,
		queueName:     DefaultRPCQueue,
		prefetchCount: 1, // Fair dispatch: worker receives 1 task at a time
		handlers:      make(map[string]RPCHandler),
		stopChan:      make(chan struct{}),
	}

	for _, opt := range opts {
		opt(worker)
	}

	// Declare Durable Request Queue
	_, err := rmq.DeclareQueue(
		worker.queueName,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare RPC request queue %s: %w", worker.queueName, err)
	}

	// Register built-in default handlers
	worker.RegisterDefaultHandlers()

	return worker, nil
}

// RegisterHandler registers a handler for a specific action/method
func (w *RPCWorker) RegisterHandler(action string, handler RPCHandler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[action] = handler
	log.Printf("[RPCWorker] Registered handler for action: %s", action)
}

// RegisterDefaultHandlers registers built-in demonstration/utility RPC handlers
func (w *RPCWorker) RegisterDefaultHandlers() {
	// 1. Ping / Health check
	w.RegisterHandler("ping", func(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
		return NewSuccessResponse(map[string]any{
			"message":   "pong",
			"timestamp": time.Now().UnixMilli(),
		})
	})

	// 2. Echo handler
	w.RegisterHandler("echo", func(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
		return NewSuccessResponse(map[string]any{
			"echo_params": string(req.Params),
			"timestamp":   time.Now().UnixMilli(),
		})
	})

	// 3. Math / Computation (Fibonacci example)
	w.RegisterHandler("compute_fibonacci", func(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
		var input struct {
			N int `json:"n"`
		}
		if len(req.Params) > 0 {
			if err := json.Unmarshal(req.Params, &input); err != nil {
				return NewErrorResponse(fmt.Sprintf("invalid params: %v", err)), nil
			}
		}

		if input.N < 0 || input.N > 45 {
			return NewErrorResponse("input 'n' must be between 0 and 45"), nil
		}

		result := fib(input.N)
		return NewSuccessResponse(map[string]any{
			"n":      input.N,
			"result": result,
		})
	})
}

// Helper fibonacci calculation
func fib(n int) int {
	if n <= 1 {
		return n
	}
	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

// Start begins consuming and processing RPC requests
func (w *RPCWorker) Start(ctx context.Context) error {
	// Set QoS prefetch for fair dispatch among competing workers
	if err := w.rmq.SetQos(w.prefetchCount, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS prefetch: %w", err)
	}

	deliveries, err := w.rmq.Consume(
		w.queueName,
		"",    // auto-generated consumer tag
		false, // manual ack for reliable Request-Reply processing
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming from RPC queue %s: %w", w.queueName, err)
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		log.Printf("[RPCWorker] Listening for requests on queue: %s (prefetch: %d)", w.queueName, w.prefetchCount)

		for {
			select {
			case <-ctx.Done():
				log.Println("[RPCWorker] Context cancelled, shutting down worker...")
				return
			case <-w.stopChan:
				log.Println("[RPCWorker] Stop signal received, shutting down worker...")
				return
			case msg, ok := <-deliveries:
				if !ok {
					log.Println("[RPCWorker] Delivery channel closed")
					return
				}
				w.processDelivery(ctx, msg)
			}
		}
	}()

	return nil
}

// processDelivery processes an individual AMQP delivery
func (w *RPCWorker) processDelivery(ctx context.Context, msg amqp.Delivery) {
	correlationID := msg.CorrelationId
	replyTo := msg.ReplyTo

	log.Printf("[RPCWorker] Received RPC request [CorrelationID: %s, ReplyTo: %s]", correlationID, replyTo)

	// Validate Request-Reply headers
	if replyTo == "" {
		log.Printf("[RPCWorker] Warning: missing ReplyTo address for CorrelationID: %s, discarding message", correlationID)
		_ = msg.Ack(false)
		return
	}

	var req RPCRequest
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		log.Printf("[RPCWorker] Error unmarshaling request: %v", err)
		w.sendReply(ctx, replyTo, correlationID, NewErrorResponse(fmt.Sprintf("malformed request: %v", err)))
		_ = msg.Ack(false)
		return
	}

	w.mu.RLock()
	handler, exists := w.handlers[req.Action]
	w.mu.RUnlock()

	var resp *RPCResponse
	if !exists {
		resp = NewErrorResponse(fmt.Sprintf("unregistered action: %s", req.Action))
	} else {
		var err error
		resp, err = handler(ctx, &req)
		if err != nil {
			log.Printf("[RPCWorker] Handler error for action %s: %v", req.Action, err)
			resp = NewErrorResponse(err.Error())
		}
	}

	if resp == nil {
		resp = NewErrorResponse("empty response generated by handler")
	}

	// Send reply back to Return Address (ReplyTo) with matching CorrelationID
	if err := w.sendReply(ctx, replyTo, correlationID, resp); err != nil {
		log.Printf("[RPCWorker] Failed to send reply to %s: %v", replyTo, err)
		_ = msg.Nack(false, true) // requeue if reply delivery failed
		return
	}

	// Successfully processed and replied, acknowledge message
	_ = msg.Ack(false)
}

// sendReply publishes the response message to the ReplyTo destination
func (w *RPCWorker) sendReply(ctx context.Context, replyTo, correlationID string, resp *RPCResponse) error {
	body, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal RPC response: %w", err)
	}

	return w.rmq.PublishRPC(ctx, DefaultRPCExchange, replyTo, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		DeliveryMode:  amqp.Transient, // replies are typically transient
		Timestamp:     time.Now(),
		Body:          body,
	})
}

// Stop gracefully stops the RPC worker
func (w *RPCWorker) Stop() {
	close(w.stopChan)
	w.wg.Wait()
	log.Println("[RPCWorker] Worker stopped successfully")
}
