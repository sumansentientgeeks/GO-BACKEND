package sfu

import (
	"os"
	"testing"
)

func TestCreatePeerWithEnv(t *testing.T) {
	testCases := []string{
		"turn:global.relay.metered.ca:80, turn:global.relay.metered.ca:80?transport=tcp, turn:global.relay.metered.ca:443 , turns:global.relay.metered.ca:443?transport=tcp ",
		"stun:stun.relay.metered.ca:80, , turn:global.relay.metered.ca:443",
		"turns:global.relay.metered.ca:443?transport=tcp",
	}

	for _, tc := range testCases {
		os.Setenv("TURN_SERVER_URL", tc)
		os.Setenv("TURN_USERNAME", "c2f54cf5c8cc9ee0f72e4646")
		os.Setenv("TURN_CREDENTIAL", "STjH/RwnVWyb74XX")

		mgr, err := NewSFUManager()
		if err != nil {
			t.Fatalf("NewSFUManager error: %v", err)
		}

		peer, err := mgr.CreatePeer("test-room", "test-user", "Test User", "speaker", func(m Message) error {
			return nil
		})
		if err != nil {
			t.Errorf("CreatePeer with '%s' failed: %v", tc, err)
		} else if peer != nil {
			t.Logf("CreatePeer succeeded for '%s'", tc)
			_ = peer.PC.Close()
		}
	}
}
