package messaging

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRPCResponseConstructors(t *testing.T) {
	// 1. Success response
	testData := map[string]string{"key": "value", "status": "active"}
	resp, err := NewSuccessResponse(testData)
	if err != nil {
		t.Fatalf("unexpected error from NewSuccessResponse: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected resp.Success to be true, got false")
	}
	if resp.Error != "" {
		t.Errorf("expected empty error, got: %s", resp.Error)
	}

	var parsed map[string]string
	if err := json.Unmarshal(resp.Data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal response data: %v", err)
	}
	if parsed["key"] != "value" {
		t.Errorf("expected parsed['key'] == 'value', got: %s", parsed["key"])
	}

	// 2. Error response
	errResp := NewErrorResponse("something went wrong")
	if errResp.Success {
		t.Errorf("expected errResp.Success to be false, got true")
	}
	if errResp.Error != "something went wrong" {
		t.Errorf("expected 'something went wrong', got: %s", errResp.Error)
	}
}

func TestCorrelationIDUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id, err := generateCorrelationID()
		if err != nil {
			t.Fatalf("generateCorrelationID failed: %v", err)
		}
		if id == "" {
			t.Fatalf("expected non-empty correlation ID")
		}
		if seen[id] {
			t.Fatalf("duplicate correlation ID detected: %s", id)
		}
		seen[id] = true
	}
}

func TestDefaultRPCHandlers(t *testing.T) {
	ctx := context.Background()

	// Simulate worker instance
	worker := &RPCWorker{
		handlers: make(map[string]RPCHandler),
	}
	worker.RegisterDefaultHandlers()

	// 1. Test "ping"
	pingHandler, exists := worker.handlers["ping"]
	if !exists {
		t.Fatalf("expected 'ping' handler to be registered")
	}
	resp, err := pingHandler(ctx, &RPCRequest{Action: "ping"})
	if err != nil {
		t.Fatalf("ping handler returned error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected ping to succeed")
	}

	var pingData map[string]any
	if err := json.Unmarshal(resp.Data, &pingData); err != nil {
		t.Fatalf("failed to unmarshal ping data: %v", err)
	}
	if pingData["message"] != "pong" {
		t.Errorf("expected 'pong', got: %v", pingData["message"])
	}

	// 2. Test "compute_fibonacci"
	fibHandler, exists := worker.handlers["compute_fibonacci"]
	if !exists {
		t.Fatalf("expected 'compute_fibonacci' handler to be registered")
	}

	// Valid input: n = 7 (fib(7) = 13)
	paramsBytes, _ := json.Marshal(map[string]int{"n": 7})
	resp, err = fibHandler(ctx, &RPCRequest{Action: "compute_fibonacci", Params: paramsBytes})
	if err != nil {
		t.Fatalf("compute_fibonacci returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success for fib(7), got error: %s", resp.Error)
	}

	var fibData struct {
		N      int `json:"n"`
		Result int `json:"result"`
	}
	if err := json.Unmarshal(resp.Data, &fibData); err != nil {
		t.Fatalf("failed to unmarshal fib data: %v", err)
	}
	if fibData.Result != 13 {
		t.Errorf("expected fib(7) == 13, got: %d", fibData.Result)
	}

	// Edge case: fib(0) = 0, fib(1) = 1
	if fib(0) != 0 || fib(1) != 1 || fib(10) != 55 {
		t.Errorf("fib function calculations incorrect")
	}

	// Invalid input: negative or too large
	negBytes, _ := json.Marshal(map[string]int{"n": -5})
	resp, _ = fibHandler(ctx, &RPCRequest{Action: "compute_fibonacci", Params: negBytes})
	if resp.Success {
		t.Errorf("expected failure for negative fibonacci input")
	}
}

func TestCustomHandlerRegistration(t *testing.T) {
	ctx := context.Background()
	worker := &RPCWorker{
		handlers: make(map[string]RPCHandler),
	}

	worker.RegisterHandler("user_greeting", func(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
		var input struct {
			Name string `json:"name"`
		}
		_ = json.Unmarshal(req.Params, &input)
		return NewSuccessResponse(map[string]string{
			"greeting": "Hello, " + input.Name + "!",
		})
	})

	handler, exists := worker.handlers["user_greeting"]
	if !exists {
		t.Fatalf("expected 'user_greeting' to be registered")
	}

	params, _ := json.Marshal(map[string]string{"name": "Alice"})
	resp, err := handler(ctx, &RPCRequest{Action: "user_greeting", Params: params})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error: %s", resp.Error)
	}

	var out map[string]string
	_ = json.Unmarshal(resp.Data, &out)
	if out["greeting"] != "Hello, Alice!" {
		t.Errorf("expected 'Hello, Alice!', got: %s", out["greeting"])
	}
}
