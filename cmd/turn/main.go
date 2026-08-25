package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pion/turn/v5"
)

func main() {
	publicIP := os.Getenv("TURN_PUBLIC_IP")
	if publicIP == "" {
		publicIP = "127.0.0.1" // Default to localhost for development
	}

	portStr := os.Getenv("TURN_PORT")
	if portStr == "" {
		portStr = "3478"
	}
	port, _ := strconv.Atoi(portStr)

	// Create a UDP listener for the TURN server
	udpListener, err := net.ListenPacket("udp4", "0.0.0.0:"+portStr)
	if err != nil {
		log.Fatalf("Failed to create TURN UDP listener: %s", err)
	}

	// Define how relay addresses are generated
	relayAddressGenerator := &turn.RelayAddressGeneratorPortRange{
		RelayAddress: net.ParseIP(publicIP),
		Address:      "0.0.0.0",
		MinPort:      50000,
		MaxPort:      50050,
	}

	realm := "localhost"

	// Configure the TURN Server
	s, err := turn.NewServer(turn.ServerConfig{
		Realm: realm,
		AuthHandler: func(ra *turn.RequestAttributes) (string, []byte, bool) {
			if ra.Username == "myuser" {
				return "myuser", turn.GenerateAuthKey("myuser", realm, "mypassword"), true
			}
			return "", nil, false
		},
		// Attach our UDP listener
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn:            udpListener,
				RelayAddressGenerator: relayAddressGenerator,
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to start TURN server: %v", err)
	}

	log.Printf("TURN server listening on port %d, Public IP: %s, Relay ports: 50000-50050", port, publicIP)

	// Wait for termination signal
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Println("Shutting down TURN server...")
	if err := s.Close(); err != nil {
		log.Fatalf("Failed to close TURN server: %v", err)
	}
}
