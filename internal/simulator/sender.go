package simulator

import (
	"encoding/json"
	"fmt"
	"net"
	"sentinelstream/internal/protocol"
)

// Sender transmits telemetry messages to a remote telemetry server.
type Sender interface {
	// Send marshals and transmits msg.
	Send(msg *protocol.TelemetryMessage) error
	// Close releases the underlying connection.
	Close() error
}

// udpSender sends each message as a single UDP datagram, matching
// ingestion.UDPListener's one-message-per-datagram framing.
type udpSender struct {
	conn net.Conn
}

// tcpSender sends each message as a newline-delimited line on a persistent
// TCP connection, matching ingestion.TCPListener's framing.
type tcpSender struct {
	conn net.Conn
}

// NewSender dials target using the given protocol ("udp" or "tcp") and
// returns a Sender ready to transmit telemetry messages.
func NewSender(protocolName, target string) (Sender, error) {
	switch protocolName {
	case "udp":
		conn, err := net.Dial("udp", target)
		if err != nil {
			return nil, fmt.Errorf("failed to dial UDP target %s: %w", target, err)
		}
		return &udpSender{conn: conn}, nil
	case "tcp":
		conn, err := net.Dial("tcp", target)
		if err != nil {
			return nil, fmt.Errorf("failed to dial TCP target %s: %w", target, err)
		}
		return &tcpSender{conn: conn}, nil
	default:
		return nil, fmt.Errorf("unsupported protocol %q (expected \"udp\" or \"tcp\")", protocolName)
	}
}

func (s *udpSender) Send(msg *protocol.TelemetryMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	if _, err := s.conn.Write(data); err != nil {
		return fmt.Errorf("failed to send UDP datagram: %w", err)
	}
	return nil
}

func (s *udpSender) Close() error {
	return s.conn.Close()
}

func (s *tcpSender) Send(msg *protocol.TelemetryMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	data = append(data, '\n')
	if _, err := s.conn.Write(data); err != nil {
		return fmt.Errorf("failed to send TCP message: %w", err)
	}
	return nil
}

func (s *tcpSender) Close() error {
	return s.conn.Close()
}
