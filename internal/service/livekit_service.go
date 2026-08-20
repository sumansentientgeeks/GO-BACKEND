package service

import (
	"os"
	"time"

	"github.com/livekit/protocol/auth"
)

type LiveKitService struct {
	apiKey    string
	apiSecret string
}

func NewLiveKitService() *LiveKitService {
	return &LiveKitService{
		apiKey:    os.Getenv("LIVEKIT_API_KEY"),
		apiSecret: os.Getenv("LIVEKIT_API_SECRET"),
	}
}

func (s *LiveKitService) GenerateToken(roomName, identity, participantName string) (string, error) {
	at := auth.NewAccessToken(s.apiKey, s.apiSecret)
	
	grant := &auth.VideoGrant{
		RoomJoin: true,
		Room:     roomName,
	}
	
	at.AddGrant(grant).
		SetIdentity(identity).
		SetName(participantName).
		SetValidFor(time.Hour)

	return at.ToJWT()
}
