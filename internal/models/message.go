package models

import "time"

type Message struct {
	ID        string    `json:"id"`
	RoomID    string    `json:"room_id"`
	SenderID  string    `json:"sender_id,omitempty"`
	Content   string    `json:"content"`
	Type      string    `json:"type"` // 'text', 'image', 'call_started', 'call_ended'
	CreatedAt time.Time `json:"created_at"`
}
