package sfu

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WSClient wraps a WebSocket connection with thread-safe write operations.
type WSClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *WSClient) WriteJSON(v interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

// HandleSFUWebSocket upgrades the HTTP connection and runs the SFU signaling session.
func HandleSFUWebSocket(manager *SFUManager) gin.HandlerFunc {
	return func(c *gin.Context) {
		roomID := c.Query("room_id")
		userID := c.Query("user_id")
		userName := c.Query("user_name")
		role := c.Query("role")

		if roomID == "" || userID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "room_id and user_id are required"})
			return
		}
		if userName == "" {
			userName = "User " + userID
		}
		if role == "" {
			role = "speaker"
		}

		conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("[SFU WS] Upgrade error: %v", err)
			return
		}

		wsClient := &WSClient{conn: conn}
		defer conn.Close()

		sendFunc := func(msg Message) error {
			return wsClient.WriteJSON(msg)
		}

		peer, err := manager.CreatePeer(roomID, userID, userName, role, sendFunc)
		if err != nil {
			log.Printf("[SFU WS] CreatePeer error: %v", err)
			return
		}

		room := manager.GetOrCreateRoom(roomID)

		// Notify existing peers that a new participant joined
		room.BroadcastExcept(userID, Message{
			Type:     "peer_joined",
			RoomID:   roomID,
			UserID:   userID,
			UserName: userName,
			Role:     role,
		})

		// Send initial room state & recording status back to this user
		participantsJSON, _ := json.Marshal(room.GetParticipants())
		_ = wsClient.WriteJSON(Message{
			Type:    "room_info",
			RoomID:  roomID,
			Payload: participantsJSON,
		})

		if room.IsRecording {
			_ = wsClient.WriteJSON(Message{
				Type:     "recording_started",
				RoomID:   roomID,
				UserID:   room.RecordedBy,
				UserName: room.RecordedBy,
			})
		}

		defer func() {
			manager.RemovePeer(roomID, userID)
			room.BroadcastAll(Message{
				Type:     "peer_left",
				RoomID:   roomID,
				UserID:   userID,
				UserName: userName,
			})
		}()

		// Read loop for signaling messages
		for {
			var msg Message
			if err := conn.ReadJSON(&msg); err != nil {
				break
			}

			switch msg.Type {
			case "offer":
				var sdpPayload SDPPayload
				if err := json.Unmarshal(msg.SDP, &sdpPayload); err != nil || sdpPayload.SDP == "" {
					var rawSDP webrtc.SessionDescription
					if err2 := json.Unmarshal(msg.SDP, &rawSDP); err2 == nil && rawSDP.SDP != "" {
						sdpPayload.Type = "offer"
						sdpPayload.SDP = rawSDP.SDP
					}
				}

				if sdpPayload.SDP == "" {
					continue
				}

				offerDesc := webrtc.SessionDescription{
					Type: webrtc.SDPTypeOffer,
					SDP:  sdpPayload.SDP,
				}

				// If in have-local-offer state (glare), rollback local offer to accept client offer
				if peer.PC.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
					_ = peer.PC.SetLocalDescription(webrtc.SessionDescription{
						Type: webrtc.SDPTypeRollback,
					})
				}

				if err := peer.PC.SetRemoteDescription(offerDesc); err != nil {
					log.Printf("[SFU WS] SetRemoteDescription offer error for %s: %v", userID, err)
					continue
				}

				// Flush buffered ICE candidates once remote description is set
				peer.DrainPendingCandidates()

				answer, err := peer.PC.CreateAnswer(nil)
				if err != nil {
					log.Printf("[SFU WS] CreateAnswer error for %s: %v", userID, err)
					continue
				}

				if err := peer.PC.SetLocalDescription(answer); err != nil {
					log.Printf("[SFU WS] SetLocalDescription answer error for %s: %v", userID, err)
					continue
				}

				answerPayload, _ := json.Marshal(SDPPayload{
					Type: answer.Type.String(),
					SDP:  answer.SDP,
				})

				_ = wsClient.WriteJSON(Message{
					Type:   "answer",
					RoomID: roomID,
					UserID: userID,
					SDP:    answerPayload,
				})

				// Trigger pending renegotiation if needed
				peer.CheckAndRunPendingRenegotiation(roomID)

			case "answer":
				var sdpPayload SDPPayload
				if err := json.Unmarshal(msg.SDP, &sdpPayload); err != nil || sdpPayload.SDP == "" {
					var rawSDP webrtc.SessionDescription
					if err2 := json.Unmarshal(msg.SDP, &rawSDP); err2 == nil && rawSDP.SDP != "" {
						sdpPayload.Type = "answer"
						sdpPayload.SDP = rawSDP.SDP
					}
				}

				if sdpPayload.SDP != "" {
					if peer.PC.SignalingState() == webrtc.SignalingStateHaveLocalOffer {
						answerDesc := webrtc.SessionDescription{
							Type: webrtc.SDPTypeAnswer,
							SDP:  sdpPayload.SDP,
						}
						if err := peer.PC.SetRemoteDescription(answerDesc); err != nil {
							log.Printf("[SFU WS] SetRemoteDescription answer error for %s: %v", userID, err)
						} else {
							peer.DrainPendingCandidates()
							// Trigger pending renegotiation if any was queued while waiting for this answer
							peer.CheckAndRunPendingRenegotiation(roomID)
						}
					}
				}

			case "ice_candidate":
				var init webrtc.ICECandidateInit
				if err := json.Unmarshal(msg.Candidate, &init); err != nil {
					continue
				}
				peer.AddCandidate(init)

			case "chat_message":
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				payload, _ := json.Marshal(map[string]interface{}{
					"user_id":   userID,
					"user_name": userName,
					"text":      msg.Text,
					"time":      time.Now().Format("15:04"),
				})
				msg.Payload = payload
				room.BroadcastAll(msg)

			case "raise_hand":
				peer.mu.Lock()
				peer.IsHandRaised = true
				peer.mu.Unlock()
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				room.BroadcastAll(msg)

			case "lower_hand":
				peer.mu.Lock()
				peer.IsHandRaised = false
				peer.mu.Unlock()
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				room.BroadcastAll(msg)

			case "media_state":
				rawState := msg.Payload
				if len(rawState) == 0 {
					rawState, _ = json.Marshal(msg)
				}
				var state struct {
					IsAudioMuted         *bool `json:"isAudioMuted"`
					IsVideoMuted         *bool `json:"isVideoMuted"`
					IsScreenSharing      *bool `json:"isScreenSharing"`
					IsAudioMutedSnake    *bool `json:"is_audio_muted"`
					IsVideoMutedSnake    *bool `json:"is_video_muted"`
					IsScreenSharingSnake *bool `json:"is_screen_sharing"`
					AudioMuted           *bool `json:"audioMuted"`
					VideoMuted           *bool `json:"videoMuted"`
				}
				if err := json.Unmarshal(rawState, &state); err == nil {
					peer.mu.Lock()
					if state.IsAudioMuted != nil {
						peer.IsAudioMuted = *state.IsAudioMuted
					} else if state.IsAudioMutedSnake != nil {
						peer.IsAudioMuted = *state.IsAudioMutedSnake
					} else if state.AudioMuted != nil {
						peer.IsAudioMuted = *state.AudioMuted
					}

					if state.IsVideoMuted != nil {
						peer.IsVideoMuted = *state.IsVideoMuted
					} else if state.IsVideoMutedSnake != nil {
						peer.IsVideoMuted = *state.IsVideoMutedSnake
					} else if state.VideoMuted != nil {
						peer.IsVideoMuted = *state.VideoMuted
					}

					if state.IsScreenSharing != nil {
						peer.IsScreenSharing = *state.IsScreenSharing
					} else if state.IsScreenSharingSnake != nil {
						peer.IsScreenSharing = *state.IsScreenSharingSnake
					}
					peer.mu.Unlock()

					if peer.IsScreenSharing && peer.PC != nil {
						for _, receiver := range peer.PC.GetReceivers() {
							if receiver.Track() != nil && receiver.Track().Kind() == webrtc.RTPCodecTypeVideo {
								_ = peer.PC.WriteRTCP([]rtcp.Packet{
									&rtcp.PictureLossIndication{MediaSSRC: uint32(receiver.Track().SSRC())},
								})
							}
						}
					}
				}
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				room.BroadcastExcept(userID, msg)

			case "recording_started":
				room.IsRecording = true
				room.RecordedBy = userName
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				room.BroadcastAll(msg)

			case "recording_stopped":
				room.IsRecording = false
				room.RecordedBy = ""
				msg.RoomID = roomID
				msg.UserID = userID
				msg.UserName = userName
				room.BroadcastAll(msg)

			default:
				log.Printf("[SFU WS] Unknown message type: %s", msg.Type)
			}
		}
	}
}
