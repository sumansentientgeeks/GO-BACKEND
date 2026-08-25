package main

import (
	"log"
	"net"
	"os"

	"github.com/pion/stun/v3"
)

func main() {
	port := os.Getenv("STUN_PORT")
	if port == "" {
		port = "3478"
	}

	addr := "0.0.0.0:" + port

	log.Printf("Starting STUN server on %s", addr)

	// Listen on UDP Port
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP: %v", err)
	}
	defer conn.Close()

	log.Println("STUN server is running and waiting for requests...")

	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Failed to read from UDP: %v", err)
			continue
		}

		go handleSTUNRequest(conn, remoteAddr, buf[:n])
	}
}

func handleSTUNRequest(conn *net.UDPConn, remoteAddr *net.UDPAddr, data []byte) {
	// Parse STUN message
	m := new(stun.Message)
	if err := m.UnmarshalBinary(data); err != nil {
		log.Printf("Failed to unmarshal STUN message from %s: %v", remoteAddr.String(), err)
		return
	}

	// We only handle STUN Binding Requests
	if m.Type.Class != stun.ClassRequest || m.Type.Method != stun.MethodBinding {
		return
	}

	// Build STUN Binding Response
	res := new(stun.Message)
	res.Build(m,
		stun.BindingSuccess,
		&stun.XORMappedAddress{
			IP:   remoteAddr.IP,
			Port: remoteAddr.Port,
		},
	)

	// Send response
	if _, err := conn.WriteToUDP(res.Raw, remoteAddr); err != nil {
		log.Printf("Failed to send STUN response to %s: %v", remoteAddr.String(), err)
	}
}
