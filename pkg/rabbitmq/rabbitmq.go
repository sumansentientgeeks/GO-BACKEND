package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ wraps the AMQP connection and channel
type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	url     string
	mu      sync.Mutex
	closed  bool
}

// Connect establishes a connection to RabbitMQ using RABBITMQ_URL env variable or fallback
func Connect() (*RabbitMQ, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
		log.Println("RABBITMQ_URL not set, using default amqp://guest:guest@localhost:5672/")
	}

	return NewRabbitMQ(url)
}

// NewRabbitMQ creates a new RabbitMQ instance with the specified connection string
func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	log.Println("RabbitMQ connection & channel established successfully")

	return &RabbitMQ{
		conn:    conn,
		channel: ch,
		url:     url,
	}, nil
}

// Channel returns the underlying AMQP channel
func (r *RabbitMQ) Channel() *amqp.Channel {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel
}

// Connection returns the underlying AMQP connection
func (r *RabbitMQ) Connection() *amqp.Connection {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conn
}

// DeclareQueue creates/verifies a queue
func (r *RabbitMQ) DeclareQueue(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return r.channel.QueueDeclare(
		name,
		durable,
		autoDelete,
		exclusive,
		noWait,
		args,
	)
}

// DeclareExchange creates/verifies an exchange
func (r *RabbitMQ) DeclareExchange(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return r.channel.ExchangeDeclare(
		name,
		kind,
		durable,
		autoDelete,
		internal,
		noWait,
		args,
	)
}

// BindQueue binds a queue to an exchange with a routing key
func (r *RabbitMQ) BindQueue(queueName, routingKey, exchangeName string, noWait bool, args amqp.Table) error {
	return r.channel.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		noWait,
		args,
	)
}

// Publish sends raw byte payload to an exchange with routing key
func (r *RabbitMQ) Publish(ctx context.Context, exchange, routingKey string, body []byte) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	return r.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
}

// PublishJSON serializes data to JSON and publishes to an exchange
func (r *RabbitMQ) PublishJSON(ctx context.Context, exchange, routingKey string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal json payload: %w", err)
	}

	return r.Publish(ctx, exchange, routingKey, body)
}

// SetQos configures quality of service / prefetch count on the channel
func (r *RabbitMQ) SetQos(prefetchCount, prefetchSize int, global bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channel == nil {
		return fmt.Errorf("rabbitmq channel is nil")
	}
	return r.channel.Qos(prefetchCount, prefetchSize, global)
}

// PublishRPC publishes an AMQP message with explicit publishing parameters (CorrelationID, ReplyTo, etc.)
func (r *RabbitMQ) PublishRPC(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}
	if msg.ContentType == "" {
		msg.ContentType = "application/json"
	}

	return r.channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)
}

// Consume starts consuming messages from a queue
func (r *RabbitMQ) Consume(queue, consumerTag string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return r.channel.Consume(
		queue,
		consumerTag,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		args,
	)
}

// Close gracefully closes the channel and connection
func (r *RabbitMQ) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	var errs []error
	if r.channel != nil {
		if err := r.channel.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing channel: %w", err))
		}
	}
	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing rabbitmq: %v", errs)
	}

	log.Println("RabbitMQ connection closed cleanly")
	return nil
}
