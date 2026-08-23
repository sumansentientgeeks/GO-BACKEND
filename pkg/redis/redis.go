package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisManager manages Redis connection and pub/sub for distributed signaling and presence
type RedisManager struct {
	Client   *goredis.Client
	IsActive bool
}

var GlobalRedis *RedisManager

// Connect initializes the Redis client using environment variables
func Connect() (*RedisManager, error) {
	redisURL := os.Getenv("REDIS_URL")
	var opts *goredis.Options

	if redisURL != "" {
		parsedOpts, err := goredis.ParseURL(redisURL)
		if err != nil {
			log.Printf("[Redis] Warning: Failed to parse REDIS_URL: %v. Falling back to REDIS_ADDR.", err)
		} else {
			opts = parsedOpts
		}
	}

	if opts == nil {
		addr := os.Getenv("REDIS_ADDR")
		if addr == "" {
			addr = "localhost:6379"
		}
		password := os.Getenv("REDIS_PASSWORD")

		opts = &goredis.Options{
			Addr:            addr,
			Password:        password,
			DB:              0,
			PoolSize:        50,
			MinIdleConns:    10,
			DialTimeout:     3 * time.Second,
			ReadTimeout:     3 * time.Second,
			WriteTimeout:    3 * time.Second,
			ConnMaxIdleTime: 5 * time.Minute,
		}
	}

	client := goredis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[Redis] Connection warning (running in standalone in-memory mode): %v", err)
		GlobalRedis = &RedisManager{Client: client, IsActive: false}
		return GlobalRedis, err
	}

	log.Println("[Redis] Connection established successfully with Redis cluster/instance")
	GlobalRedis = &RedisManager{Client: client, IsActive: true}
	return GlobalRedis, nil
}

// Publish sends a message to a Redis pub/sub channel
func (r *RedisManager) Publish(ctx context.Context, channel string, message interface{}) error {
	if r == nil || !r.IsActive || r.Client == nil {
		return nil
	}

	var payload []byte
	switch v := message.(type) {
	case []byte:
		payload = v
	case string:
		payload = []byte(v)
	default:
		data, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		payload = data
	}

	return r.Client.Publish(ctx, channel, payload).Err()
}

// Subscribe returns a Redis PubSub for the specified channels
func (r *RedisManager) Subscribe(ctx context.Context, channels ...string) *goredis.PubSub {
	if r == nil || !r.IsActive || r.Client == nil {
		return nil
	}
	return r.Client.Subscribe(ctx, channels...)
}

// SetUserPresence stores participant ephemeral presence in Redis with TTL
func (r *RedisManager) SetUserPresence(ctx context.Context, roomID, userID string, data interface{}, ttl time.Duration) error {
	if r == nil || !r.IsActive || r.Client == nil {
		return nil
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("room:%s:presence:%s", roomID, userID)
	return r.Client.Set(ctx, key, bytes, ttl).Err()
}

// RemoveUserPresence removes participant presence
func (r *RedisManager) RemoveUserPresence(ctx context.Context, roomID, userID string) error {
	if r == nil || !r.IsActive || r.Client == nil {
		return nil
	}

	key := fmt.Sprintf("room:%s:presence:%s", roomID, userID)
	return r.Client.Del(ctx, key).Err()
}
