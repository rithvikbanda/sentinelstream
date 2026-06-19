package ingestion

import (
	"encoding/json"
	"fmt"
	"net"
	"sentinelstream/internal/protocol"
	"testing"
	"time"
)

func TestTCPListener_Start_Stop(t *testing.T) {
	listener := NewTCPListener(11001, 100)

	err := listener.Start()
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}

	stats := listener.Stats()
	if !stats.IsRunning {
		t.Fatal("listener should be running")
	}

	err = listener.Stop()
	if err != nil {
		t.Fatalf("failed to stop listener: %v", err)
	}

	stats = listener.Stats()
	if stats.IsRunning {
		t.Fatal("listener should not be running after stop")
	}
}

func gpsMessageLine(sensorID string, seq uint64) []byte {
	gpsData := protocol.GPSData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Speed:     25.5,
		Heading:   180,
	}
	dataJSON, _ := json.Marshal(gpsData)

	msg := protocol.TelemetryMessage{
		SensorID:   sensorID,
		SensorType: "gps",
		Sequence:   seq,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}
	msgJSON, _ := json.Marshal(msg)
	return append(msgJSON, '\n')
}

func TestTCPListener_ReceiveValidMessage(t *testing.T) {
	port := 11002
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write(gpsMessageLine("tcp-gps-1", 1)); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	select {
	case received := <-listener.Messages():
		if received.SensorID != "tcp-gps-1" {
			t.Errorf("expected sensor_id tcp-gps-1, got %s", received.SensorID)
		}
		if received.Sequence != 1 {
			t.Errorf("expected sequence 1, got %d", received.Sequence)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	stats := listener.Stats()
	if stats.MessagesReceived != 1 {
		t.Errorf("expected 1 message received, got %d", stats.MessagesReceived)
	}
	if stats.ActiveConnections != 1 {
		t.Errorf("expected 1 active connection, got %d", stats.ActiveConnections)
	}
}

func TestTCPListener_ReceiveInvalidJSON(t *testing.T) {
	port := 11003
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("{not valid json}\n")); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	select {
	case err := <-listener.Errors():
		if err == nil {
			t.Fatal("expected an error for invalid JSON")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error")
	}

	stats := listener.Stats()
	if stats.MessagesRejected != 1 {
		t.Errorf("expected 1 message rejected, got %d", stats.MessagesRejected)
	}
}

func TestTCPListener_MultipleMessagesOneConnection(t *testing.T) {
	port := 11004
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}
	defer conn.Close()

	for i := 1; i <= 5; i++ {
		if _, err := conn.Write(gpsMessageLine("tcp-gps-multi", uint64(i))); err != nil {
			t.Fatalf("failed to write message %d: %v", i, err)
		}
	}

	receivedCount := 0
	timeout := time.After(5 * time.Second)
	for receivedCount < 5 {
		select {
		case msg := <-listener.Messages():
			if msg == nil {
				t.Fatal("received nil message")
			}
			receivedCount++
		case <-timeout:
			t.Fatalf("timeout waiting for messages (got %d/%d)", receivedCount, 5)
		}
	}

	stats := listener.Stats()
	if stats.MessagesReceived != 5 {
		t.Errorf("expected 5 messages received, got %d", stats.MessagesReceived)
	}
}

func TestTCPListener_PartialReadsAcrossWrites(t *testing.T) {
	port := 11005
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}
	defer conn.Close()

	line := gpsMessageLine("tcp-partial", 1)
	mid := len(line) / 2

	// Write the message in two chunks with a delay between them, simulating
	// a slow client whose write doesn't land in one TCP segment.
	if _, err := conn.Write(line[:mid]); err != nil {
		t.Fatalf("failed to write first half: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := conn.Write(line[mid:]); err != nil {
		t.Fatalf("failed to write second half: %v", err)
	}

	select {
	case received := <-listener.Messages():
		if received.SensorID != "tcp-partial" {
			t.Errorf("expected sensor_id tcp-partial, got %s", received.SensorID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message split across writes")
	}
}

func TestTCPListener_MultipleConcurrentConnections(t *testing.T) {
	port := 11006
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	const numConns = 5
	conns := make([]net.Conn, numConns)
	for i := 0; i < numConns; i++ {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			t.Fatalf("failed to dial TCP connection %d: %v", i, err)
		}
		conns[i] = conn
		defer conn.Close()
	}

	time.Sleep(100 * time.Millisecond)

	stats := listener.Stats()
	if stats.ActiveConnections != int64(numConns) {
		t.Errorf("expected %d active connections, got %d", numConns, stats.ActiveConnections)
	}

	for i, conn := range conns {
		sensorID := fmt.Sprintf("tcp-conn-%d", i)
		if _, err := conn.Write(gpsMessageLine(sensorID, 1)); err != nil {
			t.Fatalf("failed to write from connection %d: %v", i, err)
		}
	}

	receivedCount := 0
	timeout := time.After(5 * time.Second)
	for receivedCount < numConns {
		select {
		case msg := <-listener.Messages():
			if msg == nil {
				t.Fatal("received nil message")
			}
			receivedCount++
		case <-timeout:
			t.Fatalf("timeout waiting for messages (got %d/%d)", receivedCount, numConns)
		}
	}
}

func TestTCPListener_DetectsDisconnect(t *testing.T) {
	port := 11007
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial TCP: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if stats := listener.Stats(); stats.ActiveConnections != 1 {
		t.Fatalf("expected 1 active connection before disconnect, got %d", stats.ActiveConnections)
	}

	conn.Close()

	deadline := time.After(2 * time.Second)
	for {
		if listener.Stats().ActiveConnections == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("expected active connections to drop to 0 after disconnect")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestTCPListener_OneSlowClientDoesNotBlockOthers(t *testing.T) {
	port := 11008
	listener := NewTCPListener(port, 100)

	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	slowConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial slow connection: %v", err)
	}
	defer slowConn.Close()
	// Never write anything on slowConn - it just sits open and idle.

	fastConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial fast connection: %v", err)
	}
	defer fastConn.Close()

	if _, err := fastConn.Write(gpsMessageLine("tcp-fast", 1)); err != nil {
		t.Fatalf("failed to write from fast connection: %v", err)
	}

	select {
	case received := <-listener.Messages():
		if received.SensorID != "tcp-fast" {
			t.Errorf("expected sensor_id tcp-fast, got %s", received.SensorID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast connection's message was blocked by the idle slow connection")
	}
}

func TestTCPListenerStats_String(t *testing.T) {
	stats := TCPListenerStats{
		MessagesReceived:  100,
		MessagesRejected:  10,
		ActiveConnections: 3,
		TotalConnections:  7,
		LastErrorTime:     time.Now(),
		LastErrorMessage:  "test error",
		IsRunning:         true,
	}

	str := stats.String()
	if str == "" {
		t.Fatal("stats string should not be empty")
	}
	if !contains(str, "100") || !contains(str, "10") || !contains(str, "3") {
		t.Errorf("stats string missing expected values: %s", str)
	}
}
