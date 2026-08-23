package messaging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"example/hello/pkg/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RPCClient implements the Requestor component in the EIP Request-Reply pattern.
// It sends a request message with Return Address (ReplyTo) and CorrelationID,
// and correlates the asynchronous reply back to the synchronous caller.
type RPCClient struct {
	rmq             *rabbitmq.RabbitMQ
	requestQueue    string
	replyQueue      string
	pendingRequests sync.Map // map[string]chan *RPCResponse
	stopChan        chan struct{}
	closed          bool
	mu              sync.Mutex
}

// ClientOption configures the RPCClient
type ClientOption func(*RPCClient)

// WithClientRequestQueue sets a custom request queue for the client
func WithClientRequestQueue(queue string) ClientOption {
	return func(c *RPCClient) {
		if queue != "" {
			c.requestQueue = queue
		}
	}
}

// NewRPCClient creates a new RPC client and initializes its dedicated reply callback queue
func NewRPCClient(rmq *rabbitmq.RabbitMQ, opts ...ClientOption) (*RPCClient, error) {
	if rmq == nil {
		return nil, fmt.Errorf("rabbitmq client is required")
	}

	client := &RPCClient{
		rmq:          rmq,
		requestQueue: DefaultRPCQueue,
		stopChan:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(client)
	}

	// 1. Declare an exclusive, auto-delete callback queue for receiving replies
	q, err := rmq.DeclareQueue(
		"",    // RabbitMQ generates a unique queue name
		false, // non-durable
		true,  // auto-delete when connection closes
		true,  // exclusive to this client
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare RPC reply queue: %w", err)
	}
	client.replyQueue = q.Name
	log.Printf("[RPCClient] Initialized callback reply queue: %s", client.replyQueue)

	// 2. Start listening for incoming replies on the callback queue
	deliveries, err := rmq.Consume(
		client.replyQueue,
		"",    // consumer tag
		true,  // auto-ack for response deliveries
		true,  // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to consume from reply queue %s: %w", client.replyQueue, err)
	}

	go client.listenForReplies(deliveries)

	return client, nil
}

// listenForReplies continuously dispatches incoming replies to waiting callers by CorrelationID
func (c *RPCClient) listenForReplies(deliveries <-chan amqp.Delivery) {
	for {
		select {
		case <-c.stopChan:
			return
		case msg, ok := <-deliveries:
			if !ok {
				log.Println("[RPCClient] Reply delivery channel closed")
				return
			}

			correlationID := msg.CorrelationId
			if correlationID == "" {
				log.Println("[RPCClient] Received reply without CorrelationID, skipping")
				continue
			}

			val, exists := c.pendingRequests.Load(correlationID)
			if !exists {
				log.Printf("[RPCClient] Received reply for unknown/timed-out CorrelationID: %s", correlationID)
				continue
			}

			respChan, ok := val.(chan *RPCResponse)
			if !ok {
				log.Printf("[RPCClient] Invalid channel type in pending requests for: %s", correlationID)
				continue
			}

			var resp RPCResponse
			if err := json.Unmarshal(msg.Body, &resp); err != nil {
				log.Printf("[RPCClient] Failed to unmarshal reply payload: %v", err)
				respChan <- NewErrorResponse(fmt.Sprintf("failed to parse reply: %v", err))
				continue
			}

			respChan <- &resp
		}
	}
}

// Call executes a remote procedure call synchronously using Request-Reply
func (c *RPCClient) Call(ctx context.Context, action string, params any, result any) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	}

	correlationID, err := generateCorrelationID()
	if err != nil {
		return fmt.Errorf("failed to generate correlation ID: %w", err)
	}

	// Prepare payload
	var rawParams json.RawMessage
	if params != nil {
		bytes, err := json.Marshal(params)
		if err != nil {
			return fmt.Errorf("failed to marshal request params: %w", err)
		}
		rawParams = bytes
	}

	req := RPCRequest{
		Action:    action,
		Params:    rawParams,
		Timestamp: time.Now().UnixMilli(),
	}

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal RPC request: %w", err)
	}

	// Register response channel before publishing to avoid race condition
	respChan := make(chan *RPCResponse, 1)
	c.pendingRequests.Store(correlationID, respChan)
	defer c.pendingRequests.Delete(correlationID)

	// Publish Request message with ReplyTo and CorrelationID
	err = c.rmq.PublishRPC(ctx, DefaultRPCExchange, c.requestQueue, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		ReplyTo:       c.replyQueue,
		Timestamp:     time.Now(),
		Body:          body,
	})
	if err != nil {
		return fmt.Errorf("failed to publish RPC request: %w", err)
	}

	// Wait for reply or context timeout/cancellation
	select {
	case <-ctx.Done():
		return fmt.Errorf("RPC call timed out or cancelled: %w", ctx.Err())
	case resp := <-respChan:
		if !resp.Success {
			return fmt.Errorf("RPC worker returned error: %s", resp.Error)
		}

		if result != nil && len(resp.Data) > 0 {
			if err := json.Unmarshal(resp.Data, result); err != nil {
				return fmt.Errorf("failed to unmarshal response data into result: %w", err)
			}
		}

		return nil
	}
}

// generateCorrelationID creates a cryptographically unique correlation identifier
func generateCorrelationID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// Close closes the RPC client
func (c *RPCClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.stopChan)
}
