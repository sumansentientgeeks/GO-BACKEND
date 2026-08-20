package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"example/hello/pkg/rabbitmq"
)

const (
	ChatExchange   = "chat.events"
	ChatQueue      = "chat.messages.queue"
	ChatRoutingKey = "chat.message.new"
)

// Event represents a generic messaging event
type Event struct {
	Type      string          `json:"type"`
	RoomID    string          `json:"room_id,omitempty"`
	UserID    string          `json:"user_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
}

// EventService handles publishing and subscribing to RabbitMQ message streams
type EventService struct {
	rmq *rabbitmq.RabbitMQ
}

// NewEventService creates and initializes queues/exchanges for messaging
func NewEventService(rmq *rabbitmq.RabbitMQ) (*EventService, error) {
	if rmq == nil {
		return nil, fmt.Errorf("rabbitmq client is nil")
	}

	// 1. Declare Topic Exchange
	err := rmq.DeclareExchange(
		ChatExchange,
		"topic",
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %s: %w", ChatExchange, err)
	}

	// 2. Declare Durable Queue
	_, err = rmq.DeclareQueue(
		ChatQueue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue %s: %w", ChatQueue, err)
	}

	// 3. Bind Queue to Exchange
	err = rmq.BindQueue(
		ChatQueue,
		"chat.#", // routing key pattern
		ChatExchange,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind queue to exchange: %w", err)
	}

	return &EventService{rmq: rmq}, nil
}

// PublishEvent publishes an event to RabbitMQ
func (s *EventService) PublishEvent(ctx context.Context, routingKey string, event Event) error {
	return s.rmq.PublishJSON(ctx, ChatExchange, routingKey, event)
}

// StartConsumer starts listening for messages in background goroutine
func (s *EventService) StartConsumer(handler func(event Event) error) error {
	msgs, err := s.rmq.Consume(
		ChatQueue,
		"",    // consumer tag (auto-generated)
		false, // autoAck (manual ack for reliability)
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go func() {
		log.Printf("RabbitMQ consumer started listening on queue [%s]", ChatQueue)
		for msg := range msgs {
			var event Event
			if err := json.Unmarshal(msg.Body, &event); err != nil {
				log.Printf("Error unmarshaling message: %v", err)
				_ = msg.Nack(false, false) // discard malformed message
				continue
			}

			if err := handler(event); err != nil {
				log.Printf("Error processing message: %v", err)
				_ = msg.Nack(false, true) // requeue if processing failed
			} else {
				_ = msg.Ack(false) // acknowledge success
			}
		}
		log.Println("RabbitMQ consumer stopped")
	}()

	return nil
}
