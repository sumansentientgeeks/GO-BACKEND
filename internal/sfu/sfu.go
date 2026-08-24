package sfu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"example/hello/pkg/redis"
)

var ServerNodeID = "node_" + strconv.FormatInt(time.Now().UnixNano(), 36)

// Message is the standard signaling payload exchanged over WebSocket.
type Message struct {
	Type      string          `json:"type"`
	RoomID    string          `json:"room_id,omitempty"`
	UserID    string          `json:"user_id,omitempty"`
	UserName  string          `json:"user_name,omitempty"`
	Role      string          `json:"role,omitempty"` // "host", "speaker", "audience"
	TargetID  string          `json:"target_id,omitempty"`
	SDP       json.RawMessage `json:"sdp,omitempty"`
	Candidate json.RawMessage `json:"candidate,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Text      string          `json:"text,omitempty"`
	TrackID   string          `json:"track_id,omitempty"`
	Kind      string          `json:"kind,omitempty"`
	NodeID    string          `json:"node_id,omitempty"`
}


// SDPPayload explicitly maps the WebRTC session description.
type SDPPayload struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// ParticipantInfo holds full metadata and live media state of a participant.
type ParticipantInfo struct {
	UserID          string `json:"user_id"`
	UserName        string `json:"user_name"`
	Role            string `json:"role"`
	IsAudioMuted    bool   `json:"is_audio_muted"`
	IsVideoMuted    bool   `json:"is_video_muted"`
	IsHandRaised    bool   `json:"is_hand_raised"`
	IsScreenSharing bool   `json:"is_screen_sharing"`
}

// Peer represents a WebRTC connection with a participant in an SFU room.
type Peer struct {
	ID        string
	Name      string
	Role      string
	RoomID    string
	PC        *webrtc.PeerConnection
	SendMsg   func(msg Message) error
	Tracks    map[string]*webrtc.TrackLocalStaticRTP // Published tracks owned by this peer

	// Real-time media states
	IsAudioMuted    bool
	IsVideoMuted    bool
	IsHandRaised    bool
	IsScreenSharing bool

	pendingCandidates []webrtc.ICECandidateInit
	candidatesMu      sync.Mutex

	renegMu      sync.Mutex
	renegPending bool
	renegTimer   *time.Timer
	mu           sync.RWMutex
}

// AddCandidate buffers or adds an ICE candidate safely
func (p *Peer) AddCandidate(candidate webrtc.ICECandidateInit) {
	p.candidatesMu.Lock()
	defer p.candidatesMu.Unlock()

	if p.PC == nil || p.PC.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return
	}

	if p.PC.RemoteDescription() == nil {
		p.pendingCandidates = append(p.pendingCandidates, candidate)
		return
	}

	if err := p.PC.AddICECandidate(candidate); err != nil {
		log.Printf("[SFU WS] AddICECandidate error for %s: %v", p.ID, err)
	}
}

// DrainPendingCandidates flushes all queued ICE candidates once remote description is set
func (p *Peer) DrainPendingCandidates() {
	p.candidatesMu.Lock()
	defer p.candidatesMu.Unlock()

	if p.PC == nil || p.PC.RemoteDescription() == nil {
		return
	}

	for _, cand := range p.pendingCandidates {
		if err := p.PC.AddICECandidate(cand); err != nil {
			log.Printf("[SFU WS] Drain AddICECandidate error for %s: %v", p.ID, err)
		}
	}
	p.pendingCandidates = nil
}

// ScheduleRenegotiation triggers a debounced and state-safe SDP offer to the peer.
func (p *Peer) ScheduleRenegotiation(roomID string) {
	p.renegMu.Lock()
	defer p.renegMu.Unlock()

	if p.PC == nil || p.PC.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return
	}

	if p.renegTimer != nil {
		p.renegTimer.Stop()
	}

	p.renegTimer = time.AfterFunc(300*time.Millisecond, func() {
		p.renegMu.Lock()
		defer p.renegMu.Unlock()

		if p.PC == nil || p.PC.ConnectionState() == webrtc.PeerConnectionStateClosed {
			return
		}

		if p.PC.SignalingState() != webrtc.SignalingStateStable {
			// Mark renegotiation as pending until current state returns to stable
			p.renegPending = true
			return
		}

		p.renegPending = false
		offer, err := p.PC.CreateOffer(nil)
		if err != nil {
			log.Printf("[SFU] CreateOffer error for peer %s: %v", p.ID, err)
			return
		}

		if err := p.PC.SetLocalDescription(offer); err != nil {
			log.Printf("[SFU] SetLocalDescription error for peer %s: %v", p.ID, err)
			return
		}

		offerPayload, err := json.Marshal(SDPPayload{
			Type: offer.Type.String(),
			SDP:  offer.SDP,
		})
		if err != nil {
			return
		}

		if p.SendMsg != nil {
			_ = p.SendMsg(Message{
				Type:   "offer",
				RoomID: roomID,
				UserID: p.ID,
				SDP:    offerPayload,
			})
		}
	})
}

// CheckAndRunPendingRenegotiation is called when an answer is received and signaling state is stable.
func (p *Peer) CheckAndRunPendingRenegotiation(roomID string) {
	p.renegMu.Lock()
	defer p.renegMu.Unlock()

	if p.renegPending {
		p.renegPending = false
		go p.ScheduleRenegotiation(roomID)
	}
}

// SFURoom holds all peers, recording status, and forwarded tracks in a meeting room.
type SFURoom struct {
	ID          string
	IsRecording bool
	RecordedBy  string
	Peers       map[string]*Peer
	Tracks      map[string]*webrtc.TrackLocalStaticRTP // trackID -> *TrackLocalStaticRTP
	TrackOwners map[string]string                      // trackID -> peerID
	cancelSub   context.CancelFunc
	mu          sync.RWMutex
}


// SFUManager manages multiple rooms and WebRTC media engine settings.
type SFUManager struct {
	Rooms       map[string]*SFURoom
	MediaEngine *webrtc.MediaEngine
	API         *webrtc.API
	RTCConfig   webrtc.Configuration
	JoinQueue   chan joinTask
	mu          sync.RWMutex
}

type joinTask struct {
	roomID   string
	userID   string
	userName string
	role     string
	sendMsg  func(Message) error
	result   chan peerResult
}

type peerResult struct {
	peer *Peer
	err  error
}

// sanitizeICEUrls filters and trims URLs to ensure only valid, well-formed stun/turn URLs are passed to Pion
func sanitizeICEUrls(rawUrls []string) []string {
	valid := make([]string, 0, len(rawUrls))
	for _, u := range rawUrls {
		trimmed := strings.TrimSpace(u)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "stun:") || strings.HasPrefix(trimmed, "turn:") || strings.HasPrefix(trimmed, "turns:") {
			valid = append(valid, trimmed)
		}
	}
	return valid
}

// getRTCConfiguration builds the WebRTC STUN & TURN config supporting cloud NAT environments.
func getRTCConfiguration() webrtc.Configuration {
	iceServers := []webrtc.ICEServer{
		{
			URLs: sanitizeICEUrls([]string{
				"stun:stun.relay.metered.ca:80",
				"stun:stun.l.google.com:19302",
				"stun:stun1.l.google.com:19302",
			}),
		},
		{
			URLs: sanitizeICEUrls([]string{
				"turn:global.relay.metered.ca:80",
				"turn:global.relay.metered.ca:80?transport=tcp",
				"turn:global.relay.metered.ca:443",
				"turns:global.relay.metered.ca:443?transport=tcp",
			}),
			Username:       "c2f54cf5c8cc9ee0f72e4646",
			Credential:     "STjH/RwnVWyb74XX",
			CredentialType: webrtc.ICECredentialTypePassword,
		},
	}

	// Support custom override from environment variables if set
	turnURL := os.Getenv("TURN_SERVER_URL")
	if turnURL == "" {
		turnURL = os.Getenv("TURN_URL")
	}
	turnUser := os.Getenv("TURN_USERNAME")
	turnCred := os.Getenv("TURN_CREDENTIAL")
	if turnCred == "" {
		turnCred = os.Getenv("TURN_PASSWORD")
	}

	if turnURL != "" && turnUser != "" && turnCred != "" {
		rawList := strings.Split(turnURL, ",")
		cleaned := sanitizeICEUrls(rawList)
		if len(cleaned) > 0 {
			iceServers = append([]webrtc.ICEServer{{
				URLs:           cleaned,
				Username:       strings.TrimSpace(turnUser),
				Credential:     strings.TrimSpace(turnCred),
				CredentialType: webrtc.ICECredentialTypePassword,
			}}, iceServers...)
		}
	}

	return webrtc.Configuration{
		ICEServers:         iceServers,
		ICETransportPolicy: webrtc.ICETransportPolicyAll,
		BundlePolicy:       webrtc.BundlePolicyBalanced,
		RTCPMuxPolicy:      webrtc.RTCPMuxPolicyRequire,
	}
}

// buildMediaEngine constructs a WebRTC MediaEngine configured with universal Opus audio and video codecs.
func buildMediaEngine() (*webrtc.MediaEngine, error) {
	m := &webrtc.MediaEngine{}

	// 1. Register default standard WebRTC codecs (Opus 48kHz stereo, VP8, H264, VP9, PCMU, PCMA)
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}

	// 2. Register RFC 6464 Audio Level Header Extension (for active speaker detection)
	if err := m.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{
		URI: "urn:ietf:params:rtp-hdrext:ssrc-audio-level",
	}, webrtc.RTPCodecTypeAudio); err != nil {
		log.Printf("[SFU] Warning: failed to register audio-level header extension: %v", err)
	}

	// 3. Register Transport-Wide Congestion Control Header Extension
	if err := m.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{
		URI: "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
	}, webrtc.RTPCodecTypeVideo); err != nil {
		log.Printf("[SFU] Warning: failed to register transport-cc header extension: %v", err)
	}

	return m, nil
}

// NewSFUManager initializes WebRTC media codecs, interceptors, setting engine, and SFU engine.
func NewSFUManager() (*SFUManager, error) {
	m, err := buildMediaEngine()
	if err != nil {
		return nil, err
	}

	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(m, ir); err != nil {
		return nil, err
	}


	se := webrtc.SettingEngine{}

	// Configure NAT 1:1 Public IP if deployed behind cloud NAT/Docker/VPS
	natIP := os.Getenv("NAT_1TO1_IP")
	if natIP == "" {
		natIP = os.Getenv("PUBLIC_IP")
	}
	if natIP != "" {
		log.Printf("[SFU] Configuring NAT 1:1 IP: %s", natIP)
		se.SetNAT1To1IPs([]string{natIP}, webrtc.ICECandidateTypeHost)
	}

	// Configure UDP Port range if specified
	minPortStr := os.Getenv("UDP_PORT_MIN")
	maxPortStr := os.Getenv("UDP_PORT_MAX")
	if minPortStr != "" && maxPortStr != "" {
		minPort, err1 := strconv.ParseUint(minPortStr, 10, 16)
		maxPort, err2 := strconv.ParseUint(maxPortStr, 10, 16)
		if err1 == nil && err2 == nil && minPort < maxPort {
			log.Printf("[SFU] Configuring Ephemeral UDP Port Range: %d - %d", minPort, maxPort)
			if err := se.SetEphemeralUDPPortRange(uint16(minPort), uint16(maxPort)); err != nil {
				log.Printf("[SFU] Warning: failed to set UDP port range: %v", err)
			}
		}
	}

	// Lenient ICE timeouts for cloud environments with NAT and network latency
	se.SetICETimeouts(25*time.Second, 50*time.Second, 3*time.Second)

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(m),
		webrtc.WithInterceptorRegistry(ir),
		webrtc.WithSettingEngine(se),
	)

	sfu := &SFUManager{
		Rooms:       make(map[string]*SFURoom),
		MediaEngine: m,
		API:         api,
		RTCConfig:   getRTCConfiguration(),
		JoinQueue:   make(chan joinTask, 10000),
	}

	// Start 16 background workers for processing concurrent joins
	for i := 0; i < 16; i++ {
		go sfu.joinWorker()
	}

	return sfu, nil
}

func (s *SFUManager) joinWorker() {
	for task := range s.JoinQueue {
		peer, err := s.createPeerInternal(task.roomID, task.userID, task.userName, task.role, task.sendMsg)
		task.result <- peerResult{peer: peer, err: err}
	}
}

// GetOrCreateRoom returns or creates a room by ID.
func (s *SFUManager) GetOrCreateRoom(roomID string) *SFURoom {
	s.mu.Lock()
	defer s.mu.Unlock()

	if room, ok := s.Rooms[roomID]; ok {
		return room
	}

	ctx, cancel := context.WithCancel(context.Background())
	room := &SFURoom{
		ID:          roomID,
		Peers:       make(map[string]*Peer),
		Tracks:      make(map[string]*webrtc.TrackLocalStaticRTP),
		TrackOwners: make(map[string]string),
		cancelSub:   cancel,
	}
	s.Rooms[roomID] = room

	// Start Redis cluster pub/sub listener for cross-node signaling if Redis is active
	if redis.GlobalRedis != nil && redis.GlobalRedis.IsActive {
		go room.subscribeRedisSignals(ctx)
	}

	return room
}

// RemoveRoomIfEmpty removes a room from the manager if empty.
func (s *SFUManager) RemoveRoomIfEmpty(roomID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if room, ok := s.Rooms[roomID]; ok {
		room.mu.RLock()
		empty := len(room.Peers) == 0
		room.mu.RUnlock()

		if empty {
			if room.cancelSub != nil {
				room.cancelSub()
			}
			delete(s.Rooms, roomID)
		}
	}
}


// CreatePeer dispatches peer creation via high-concurrency worker pool.
func (s *SFUManager) CreatePeer(roomID, userID, userName, role string, sendMsg func(Message) error) (*Peer, error) {
	resChan := make(chan peerResult, 1)
	s.JoinQueue <- joinTask{
		roomID:   roomID,
		userID:   userID,
		userName: userName,
		role:     role,
		sendMsg:  sendMsg,
		result:   resChan,
	}
	res := <-resChan
	return res.peer, res.err
}

func (s *SFUManager) createPeerInternal(roomID, userID, userName, role string, sendMsg func(Message) error) (*Peer, error) {
	pc, err := s.API.NewPeerConnection(s.RTCConfig)
	if err != nil {
		return nil, err
	}

	if role == "" {
		role = "speaker"
	}

	peer := &Peer{
		ID:           userID,
		Name:         userName,
		Role:         role,
		RoomID:       roomID,
		PC:           pc,
		SendMsg:      sendMsg,
		Tracks:       make(map[string]*webrtc.TrackLocalStaticRTP),
		IsAudioMuted: false,
		IsVideoMuted: false,
	}

	room := s.GetOrCreateRoom(roomID)

	// Send ICE candidate to client when generated by server
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		cJSON, err := json.Marshal(candidate.ToJSON())
		if err != nil {
			return
		}
		_ = sendMsg(Message{
			Type:      "ice_candidate",
			RoomID:    roomID,
			UserID:    userID,
			Candidate: cJSON,
		})
	})

	// Handle incoming published tracks from this peer
	pc.OnTrack(func(remoteTrack *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[SFU] Received track %s (Kind: %s, SSRC: %d) from peer %s", remoteTrack.ID(), remoteTrack.Kind().String(), remoteTrack.SSRC(), userID)

		// Create a local track using userID as StreamID so client maps remote streams seamlessly
		localTrack, err := webrtc.NewTrackLocalStaticRTP(
			remoteTrack.Codec().RTPCodecCapability,
			remoteTrack.ID(),
			userID,
		)
		if err != nil {
			log.Printf("[SFU] Failed to create local track: %v", err)
			return
		}

		peer.mu.Lock()
		peer.Tracks[localTrack.ID()] = localTrack
		peer.mu.Unlock()

		room.mu.Lock()
		room.Tracks[localTrack.ID()] = localTrack
		room.TrackOwners[localTrack.ID()] = userID

		// Add this new track to all other existing peers in the room
		for id, otherPeer := range room.Peers {
			if id != userID && otherPeer.PC != nil && otherPeer.PC.ConnectionState() != webrtc.PeerConnectionStateClosed {
				if _, err := otherPeer.PC.AddTrack(localTrack); err != nil {
					log.Printf("[SFU] Error adding track %s to peer %s: %v", localTrack.ID(), otherPeer.ID, err)
				} else {
					// Trigger debounced renegotiation
					otherPeer.ScheduleRenegotiation(roomID)
				}
			}
		}
		room.mu.Unlock()

		// Request periodic PLI keyframes for video tracks so subscribers render cleanly and immediately
		if remoteTrack.Kind() == webrtc.RTPCodecTypeVideo {
			// Trigger immediate initial PLI keyframe request
			_ = pc.WriteRTCP([]rtcp.Packet{
				&rtcp.PictureLossIndication{MediaSSRC: uint32(remoteTrack.SSRC())},
			})

			go func() {
				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					if pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
						return
					}
					_ = pc.WriteRTCP([]rtcp.Packet{
						&rtcp.PictureLossIndication{MediaSSRC: uint32(remoteTrack.SSRC())},
					})
				}
			}()
		}

		// Read RTP packets from remoteTrack and fan out to localTrack
		// CRITICAL: Do not terminate on writeErr (e.g. io.ErrClosedPipe when 0 subscribers exist)
		// Only terminate when the publisher stream itself is closed (io.EOF or read error).
		go func() {
			var packetCount uint64
			for {
				pkt, _, readErr := remoteTrack.ReadRTP()
				if readErr != nil {
					if errors.Is(readErr, io.EOF) {
						log.Printf("[SFU] Stream closed (EOF) for track %s (%s) from %s", remoteTrack.ID(), remoteTrack.Kind().String(), userID)
						return
					}
					return
				}
				packetCount++
				if packetCount == 1 || packetCount%1000 == 0 {
					log.Printf("[SFU] Forwarding RTP track %s (%s) packet #%d from %s", remoteTrack.ID(), remoteTrack.Kind().String(), packetCount, userID)
				}
				// Write packet to subscribers (if no subscribers yet, this will safely return an error without killing our read loop)
				_ = localTrack.WriteRTP(pkt)
			}
		}()
	})

	// Add peer to room and subscribe them to existing room tracks
	room.mu.Lock()
	hasExistingTracks := false
	for trackID, existingTrack := range room.Tracks {
		ownerID := room.TrackOwners[trackID]
		if ownerID != userID && existingTrack != nil {
			if _, err := pc.AddTrack(existingTrack); err != nil {
				log.Printf("[SFU] Error subscribing peer %s to existing track %s: %v", userID, trackID, err)
			} else {
				hasExistingTracks = true
				// Request immediate PLI from the publisher of this track
				if ownerPeer, ok := room.Peers[ownerID]; ok && ownerPeer.PC != nil {
					for _, receiver := range ownerPeer.PC.GetReceivers() {
						if receiver.Track() != nil && receiver.Track().Kind() == webrtc.RTPCodecTypeVideo {
							_ = ownerPeer.PC.WriteRTCP([]rtcp.Packet{
								&rtcp.PictureLossIndication{MediaSSRC: uint32(receiver.Track().SSRC())},
							})
						}
					}
				}
			}
		}
	}
	room.Peers[userID] = peer
	room.mu.Unlock()

	if hasExistingTracks {
		peer.renegMu.Lock()
		peer.renegPending = true
		peer.renegMu.Unlock()
	}

	return peer, nil
}

// RemovePeer closes peer connections and cleans up tracks from the room.
func (s *SFUManager) RemovePeer(roomID, userID string) {
	room := s.GetOrCreateRoom(roomID)
	room.mu.Lock()
	peer, exists := room.Peers[userID]
	if !exists {
		room.mu.Unlock()
		return
	}

	// Collect tracks to remove
	tracksToRemove := make([]*webrtc.TrackLocalStaticRTP, 0, len(peer.Tracks))
	peer.mu.Lock()
	for trackID, track := range peer.Tracks {
		if track != nil {
			tracksToRemove = append(tracksToRemove, track)
		}
		delete(room.Tracks, trackID)
		delete(room.TrackOwners, trackID)
	}
	peer.mu.Unlock()
	delete(room.Peers, userID)

	// Clean up senders from remaining peers in the room
	for otherID, otherPeer := range room.Peers {
		if otherID != userID && otherPeer != nil && otherPeer.PC != nil {
			if otherPeer.PC.ConnectionState() != webrtc.PeerConnectionStateClosed {
				for _, sender := range otherPeer.PC.GetSenders() {
					if sender != nil {
						track := sender.Track()
						if track != nil {
							for _, tr := range tracksToRemove {
								if tr != nil && track.ID() == tr.ID() {
									_ = otherPeer.PC.RemoveTrack(sender)
									break
								}
							}
						}
					}
				}
				otherPeer.ScheduleRenegotiation(roomID)
			}
		}
	}
	room.mu.Unlock()

	if peer.PC != nil {
		_ = peer.PC.Close()
	}
	log.Printf("[SFU] Peer %s left room %s", userID, roomID)

	s.RemoveRoomIfEmpty(roomID)
}

// subscribeRedisSignals subscribes to Redis pub/sub channel for cross-node message routing
func (room *SFURoom) subscribeRedisSignals(ctx context.Context) {
	if redis.GlobalRedis == nil || !redis.GlobalRedis.IsActive {
		return
	}
	channel := fmt.Sprintf("room:%s:signals", room.ID)
	pubsub := redis.GlobalRedis.Subscribe(ctx, channel)
	if pubsub == nil {
		return
	}
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case rMsg, ok := <-ch:
			if !ok {
				return
			}
			var signalMsg Message
			if err := json.Unmarshal([]byte(rMsg.Payload), &signalMsg); err == nil {
				// Only forward signals generated by other server nodes in cluster
				if signalMsg.NodeID != ServerNodeID {
					room.BroadcastLocalAll(signalMsg)
				}
			}
		}
	}
}

// BroadcastLocalAll sends a message directly to all local peers in the room without republishing to Redis.
func (room *SFURoom) BroadcastLocalAll(msg Message) {
	room.mu.RLock()
	defer room.mu.RUnlock()

	for _, peer := range room.Peers {
		if peer.SendMsg != nil {
			_ = peer.SendMsg(msg)
		}
	}
}

// BroadcastExcept sends a message to all peers in the room except the sender, and publishes to Redis cluster.
func (room *SFURoom) BroadcastExcept(senderID string, msg Message) {
	msg.NodeID = ServerNodeID

	// 1. Send to local connected peers
	room.mu.RLock()
	for id, peer := range room.Peers {
		if id != senderID && peer.SendMsg != nil {
			_ = peer.SendMsg(msg)
		}
	}
	room.mu.RUnlock()

	// 2. Publish to Redis cluster for remote peers on other nodes
	if redis.GlobalRedis != nil && redis.GlobalRedis.IsActive {
		go func() {
			channel := fmt.Sprintf("room:%s:signals", room.ID)
			_ = redis.GlobalRedis.Publish(context.Background(), channel, msg)
		}()
	}
}

// BroadcastAll sends a message to all peers in the room, and publishes to Redis cluster.
func (room *SFURoom) BroadcastAll(msg Message) {
	msg.NodeID = ServerNodeID

	// 1. Send to local connected peers
	room.mu.RLock()
	for _, peer := range room.Peers {
		if peer.SendMsg != nil {
			_ = peer.SendMsg(msg)
		}
	}
	room.mu.RUnlock()

	// 2. Publish to Redis cluster for remote peers on other nodes
	if redis.GlobalRedis != nil && redis.GlobalRedis.IsActive {
		go func() {
			channel := fmt.Sprintf("room:%s:signals", room.ID)
			_ = redis.GlobalRedis.Publish(context.Background(), channel, msg)
		}()
	}
}


// GetParticipants returns full participant info for all active peers in the room.
func (room *SFURoom) GetParticipants() []ParticipantInfo {
	room.mu.RLock()
	defer room.mu.RUnlock()

	list := make([]ParticipantInfo, 0, len(room.Peers))
	for id, p := range room.Peers {
		p.mu.RLock()
		list = append(list, ParticipantInfo{
			UserID:          id,
			UserName:        p.Name,
			Role:            p.Role,
			IsAudioMuted:    p.IsAudioMuted,
			IsVideoMuted:    p.IsVideoMuted,
			IsHandRaised:    p.IsHandRaised,
			IsScreenSharing: p.IsScreenSharing,
		})
		p.mu.RUnlock()
	}
	return list
}
