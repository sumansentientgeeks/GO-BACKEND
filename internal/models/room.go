package models

import "time"

type Room struct {
	ID        string    `json:"id"`
	Name      string    `json:"name,omitempty"`
	Type      string    `json:"type"` // 'direct', 'group'
	CreatedAt time.Time `json:"created_at"`
}

type RoomParticipant struct {
	RoomID   string    `json:"room_id"`
	UserID   string    `json:"user_id"`
	Role     string    `json:"role"` // 'admin', 'member'
	JoinedAt time.Time `json:"joined_at"`
}
