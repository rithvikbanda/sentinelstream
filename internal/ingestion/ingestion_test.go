package ingestion

import (
	"encoding/json"
	"fmt"
	"net"
	"sentinelstream/internal/protocol"
	"testing"
	"time"
)

func TestUDPListener_Start_Stop(t *testing.T) {
	listener := NewUDPListener(9999, 100)

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

func TestUDPListener_ReceiveValidMessage(t *testing.T) {
	port := 10001
	listener := NewUDPListener(port, 100)

	err := listener.Start()
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	// Give the listener a moment to start
	time.Sleep(100 * time.Millisecond)

	// Send a valid GPS message
	gpsData := protocol.GPSData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Speed:     25.5,
		Heading:   180,
	}
	dataJSON, _ := json.Marshal(gpsData)

	msg := protocol.TelemetryMessage{
		SensorID:   "gps-test-1",
		SensorType: "gps",
		Sequence:   1,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}

	msgJSON, _ := json.Marshal(msg)

	// Send the message via UDP
	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial UDP: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write(msgJSON)
	if err != nil {
		t.Fatalf("failed to send UDP message: %v", err)
	}

	// Wait for message to be received
	select {
	case received := <-listener.Messages():
		if received.SensorID != "gps-test-1" {
			t.Errorf("expected sensor_id gps-test-1, got %s", received.SensorID)
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
}

func TestUDPListener_ReceiveInvalidJSON(t *testing.T) {
	port := 10002
	listener := NewUDPListener(port, 100)

	err := listener.Start()
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	// Send invalid JSON
	invalidJSON := []byte(`{invalid json}`)

	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial UDP: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write(invalidJSON)
	if err != nil {
		t.Fatalf("failed to send UDP message: %v", err)
	}

	// Wait for error
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

func TestUDPListener_ReceiveInvalidSensorData(t *testing.T) {
	port := 10003
	listener := NewUDPListener(port, 100)

	err := listener.Start()
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	// Send a drone message with invalid battery (> 100).
	// Note: The listener accepts this because it only validates the envelope.
	// Sensor-specific validation happens downstream in the processor.
	droneData := protocol.DroneData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Battery:   145, // Invalid: > 100, but listener accepts it
		Signal:    95,
	}
	dataJSON, _ := json.Marshal(droneData)

	msg := protocol.TelemetryMessage{
		SensorID:   "drone-test-1",
		SensorType: "drone",
		Sequence:   1,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}

	msgJSON, _ := json.Marshal(msg)

	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial UDP: %v", err)
	}
	defer conn.Close()

	_, err = conn.Write(msgJSON)
	if err != nil {
		t.Fatalf("failed to send UDP message: %v", err)
	}

	// The listener should accept this message even though drone data is invalid
	// Validation of sensor-specific fields happens in the processor, not the listener
	select {
	case msg := <-listener.Messages():
		if msg == nil {
			t.Fatal("expected to receive message")
		}
		if msg.SensorID != "drone-test-1" {
			t.Errorf("expected drone-test-1, got %s", msg.SensorID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}

	stats := listener.Stats()
	if stats.MessagesReceived != 1 {
		t.Errorf("expected 1 message received, got %d", stats.MessagesReceived)
	}
}

func TestUDPListener_ReceiveMultipleMessages(t *testing.T) {
	port := 10004
	listener := NewUDPListener(port, 100)

	err := listener.Start()
	if err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()

	time.Sleep(100 * time.Millisecond)

	// Send 5 valid GPS messages
	conn, err := net.Dial("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to dial UDP: %v", err)
	}
	defer conn.Close()

	for i := 1; i <= 5; i++ {
		gpsData := protocol.GPSData{
			Latitude:  47.674,
			Longitude: -122.121,
			Altitude:  152.4,
			Speed:     float64(i * 10),
			Heading:   180,
		}
		dataJSON, _ := json.Marshal(gpsData)

		msg := protocol.TelemetryMessage{
			SensorID:   "gps-test-1",
			SensorType: "gps",
			Sequence:   uint64(i),
			Timestamp:  time.Now().UTC(),
			Data:       dataJSON,
		}

		msgJSON, _ := json.Marshal(msg)
		_, err = conn.Write(msgJSON)
		if err != nil {
			t.Fatalf("failed to send UDP message %d: %v", i, err)
		}
	}

	// Wait for all messages
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

func TestListenerStats_String(t *testing.T) {
	stats := ListenerStats{
		MessagesReceived:  100,
		MessagesRejected:  10,
		LastErrorTime:     time.Now(),
		LastErrorMessage:  "test error",
		IsRunning:         true,
	}

	str := stats.String()
	if str == "" {
		t.Fatal("stats string should not be empty")
	}

	// Check that the string contains expected information
	if !contains(str, "100") || !contains(str, "10") {
		t.Errorf("stats string missing expected values: %s", str)
	}
}

func contains(s, substr string) bool {
	for i := 0; i < len(s)-len(substr)+1; i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
