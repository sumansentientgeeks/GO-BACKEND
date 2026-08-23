package messaging

import (
	"context"
	"encoding/json"
	"time"
)

const (
	// DefaultRPCQueue is the standard request queue for RPC workers
	DefaultRPCQueue = "rpc.requests.queue"

	// DefaultRPCExchange is the AMQP default exchange for direct reply routing
	DefaultRPCExchange = ""
)

// RPCRequest represents the incoming request payload in the Request-Reply pattern
type RPCRequest struct {
	Action    string            `json:"action"`
	Params    json.RawMessage   `json:"params,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp int64             `json:"timestamp"`
}

// RPCResponse represents the reply payload returned to the requestor
type RPCResponse struct {
	Success   bool            `json:"success"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     string          `json:"error,omitempty"`
	Timestamp int64           `json:"timestamp"`
}

// NewSuccessResponse creates a successful RPC response with serialized data
func NewSuccessResponse(data any) (*RPCResponse, error) {
	var raw json.RawMessage
	if data != nil {
		bytes, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		raw = bytes
	}

	return &RPCResponse{
		Success:   true,
		Data:      raw,
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// NewErrorResponse creates a failure RPC response with an error message
func NewErrorResponse(errMsg string) *RPCResponse {
	return &RPCResponse{
		Success:   false,
		Error:     errMsg,
		Timestamp: time.Now().UnixMilli(),
	}
}

// RPCHandler defines the signature for RPC worker action handlers
type RPCHandler func(ctx context.Context, req *RPCRequest) (*RPCResponse, error)
