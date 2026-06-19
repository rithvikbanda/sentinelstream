package simulator

import (
	"fmt"
	"sentinelstream/internal/ingestion"
	"testing"
	"time"
)

func TestNewSender_UnsupportedProtocol(t *testing.T) {
	_, err := NewSender("carrier-pigeon", "127.0.0.1:9999")
	if err == nil {
		t.Fatal("expected an error for an unsupported protocol")
	}
}

func TestUDPSender_DeliversToRealListener(t *testing.T) {
	port := 12001
	listener := ingestion.NewUDPListener(port, 10)
	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()
	time.Sleep(100 * time.Millisecond)

	sender, err := NewSender("udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close()

	gen := NewGPSSensorGenerator("sim-udp-1", 1)
	if err := sender.Send(gen.Next()); err != nil {
		t.Fatalf("failed to send message: %v", err)
	}

	select {
	case msg := <-listener.Messages():
		if msg.SensorID != "sim-udp-1" {
			t.Errorf("expected sensor_id sim-udp-1, got %s", msg.SensorID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message to arrive at listener")
	}
}

func TestTCPSender_DeliversToRealListener(t *testing.T) {
	port := 12002
	listener := ingestion.NewTCPListener(port, 10)
	if err := listener.Start(); err != nil {
		t.Fatalf("failed to start listener: %v", err)
	}
	defer listener.Stop()
	time.Sleep(100 * time.Millisecond)

	sender, err := NewSender("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close()

	gen := NewDroneSensorGenerator("sim-tcp-1", 1)
	for i := 0; i < 3; i++ {
		if err := sender.Send(gen.Next()); err != nil {
			t.Fatalf("failed to send message %d: %v", i, err)
		}
	}

	received := 0
	timeout := time.After(2 * time.Second)
	for received < 3 {
		select {
		case msg := <-listener.Messages():
			if msg.SensorID != "sim-tcp-1" {
				t.Errorf("expected sensor_id sim-tcp-1, got %s", msg.SensorID)
			}
			received++
		case <-timeout:
			t.Fatalf("timeout waiting for messages (got %d/3)", received)
		}
	}
}
