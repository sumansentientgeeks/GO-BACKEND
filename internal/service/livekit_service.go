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
	apiKey := s.apiKey
	if apiKey == "" {
		apiKey = os.Getenv("LIVEKIT_API_KEY")
	}
	apiSecret := s.apiSecret
	if apiSecret == "" {
		apiSecret = os.Getenv("LIVEKIT_API_SECRET")
	}

	at := auth.NewAccessToken(apiKey, apiSecret)

	canPublish := true
	canSubscribe := true
	canPublishData := true

	grant := &auth.VideoGrant{
		RoomJoin:       true,
		Room:           roomName,
		CanPublish:     &canPublish,
		CanSubscribe:   &canSubscribe,
		CanPublishData: &canPublishData,
	}

	at.AddGrant(grant).
		SetIdentity(identity).
		SetName(participantName).
		SetValidFor(24 * time.Hour)

	return at.ToJWT()
}
