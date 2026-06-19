package processor

import (
	"context"
	"encoding/json"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"testing"
	"time"
)

func TestProcessor_StartStop(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}

	stats := proc.Stats()
	if !stats.IsRunning {
		t.Fatal("processor should be running")
	}

	err = proc.Stop()
	if err != nil {
		t.Fatalf("failed to stop processor: %v", err)
	}

	stats = proc.Stats()
	if stats.IsRunning {
		t.Fatal("processor should not be running after stop")
	}
}

func TestProcessor_Enqueue(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// Create a GPS message
	gpsData := protocol.GPSData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Speed:     25.5,
		Heading:   180,
	}
	dataJSON, _ := json.Marshal(gpsData)

	msg := &protocol.TelemetryMessage{
		SensorID:   "gps-1",
		SensorType: "gps",
		Sequence:   1,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}

	err = proc.Enqueue(msg)
	if err != nil {
		t.Fatalf("failed to enqueue message: %v", err)
	}

	// Give workers time to process
	time.Sleep(100 * time.Millisecond)

	stats := proc.Stats()
	if stats.TotalMessagesEnqueued != 1 {
		t.Errorf("expected 1 enqueued, got %d", stats.TotalMessagesEnqueued)
	}
	if stats.TotalMessagesProcessed != 1 {
		t.Errorf("expected 1 processed, got %d", stats.TotalMessagesProcessed)
	}
	if stats.ProcessingLatency.Count != 1 {
		t.Errorf("expected 1 processing latency sample, got %d", stats.ProcessingLatency.Count)
	}
	if stats.EndToEndLatency.Count != 1 {
		t.Errorf("expected 1 end-to-end latency sample, got %d", stats.EndToEndLatency.Count)
	}
}

func TestProcessor_MultipleMessages(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 4,
		QueueSize:  1000,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// Enqueue 100 messages
	for i := 0; i < 100; i++ {
		gpsData := protocol.GPSData{
			Latitude:  47.674 + float64(i)*0.001,
			Longitude: -122.121,
			Altitude:  152.4,
			Speed:     25.5,
			Heading:   180,
		}
		dataJSON, _ := json.Marshal(gpsData)

		msg := &protocol.TelemetryMessage{
			SensorID:   "gps-1",
			SensorType: "gps",
			Sequence:   uint64(i + 1),
			Timestamp:  time.Now().UTC(),
			Data:       dataJSON,
		}

		err := proc.Enqueue(msg)
		if err != nil {
			t.Fatalf("failed to enqueue message %d: %v", i, err)
		}
	}

	// Wait for processing
	err = proc.WaitForQuiescence(5 * time.Second)
	if err != nil {
		t.Fatalf("failed waiting for quiescence: %v", err)
	}

	stats := proc.Stats()
	if stats.TotalMessagesProcessed != 100 {
		t.Errorf("expected 100 processed, got %d", stats.TotalMessagesProcessed)
	}
}

func TestProcessor_InvalidMessage(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// Create a drone message with an out-of-range battery value - a
	// well-formed message that should be flagged as an anomaly, not a
	// processing error.
	droneData := protocol.DroneData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Battery:   150, // Invalid: > 100
		Signal:    95,
	}
	dataJSON, _ := json.Marshal(droneData)

	msg := &protocol.TelemetryMessage{
		SensorID:   "drone-1",
		SensorType: "drone",
		Sequence:   1,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}

	err = proc.Enqueue(msg)
	if err != nil {
		t.Fatalf("failed to enqueue message: %v", err)
	}

	// Give workers time to process
	time.Sleep(100 * time.Millisecond)

	stats := proc.Stats()
	if stats.TotalMessagesEnqueued != 1 {
		t.Errorf("expected 1 enqueued, got %d", stats.TotalMessagesEnqueued)
	}
	if stats.TotalAnomalies != 1 {
		t.Errorf("expected 1 anomaly, got %d", stats.TotalAnomalies)
	}
	if stats.TotalErrors != 0 {
		t.Errorf("expected 0 errors, got %d", stats.TotalErrors)
	}
}

func TestProcessor_TryEnqueue(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 1,
		QueueSize:  2, // Very small queue
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// A worker is draining the queue concurrently, so we can't assume it's
	// still full after exactly QueueSize enqueues - keep pushing (faster
	// than the worker can process) until a queue-full event is observed.
	deadline := time.Now().Add(2 * time.Second)
	sawQueueFull := false
	for seq := uint64(1); !sawQueueFull; seq++ {
		gpsData := protocol.GPSData{
			Latitude:  47.674,
			Longitude: -122.121,
			Altitude:  152.4,
			Speed:     25.5,
			Heading:   180,
		}
		dataJSON, _ := json.Marshal(gpsData)

		msg := &protocol.TelemetryMessage{
			SensorID:   "gps-1",
			SensorType: "gps",
			Sequence:   seq,
			Timestamp:  time.Now().UTC(),
			Data:       dataJSON,
		}

		if err := proc.TryEnqueue(msg); err != nil {
			sawQueueFull = true
		}

		if !sawQueueFull && time.Now().After(deadline) {
			t.Fatal("expected a queue-full event but the queue never filled")
		}
	}

	stats := proc.Stats()
	if stats.QueueFullEvents == 0 {
		t.Errorf("expected at least 1 queue full event, got %d", stats.QueueFullEvents)
	}
}

func TestProcessor_WorkerStats(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// Enqueue some messages
	for i := 0; i < 10; i++ {
		gpsData := protocol.GPSData{
			Latitude:  47.674,
			Longitude: -122.121,
			Altitude:  152.4,
			Speed:     25.5,
			Heading:   180,
		}
		dataJSON, _ := json.Marshal(gpsData)

		msg := &protocol.TelemetryMessage{
			SensorID:   "gps-1",
			SensorType: "gps",
			Sequence:   uint64(i + 1),
			Timestamp:  time.Now().UTC(),
			Data:       dataJSON,
		}

		proc.Enqueue(msg)
	}

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	stats := proc.Stats()
	if len(stats.WorkerStats) != 2 {
		t.Errorf("expected 2 workers, got %d", len(stats.WorkerStats))
	}

	for i, ws := range stats.WorkerStats {
		if ws.ID != i {
			t.Errorf("expected worker ID %d, got %d", i, ws.ID)
		}
	}
}

func TestProcessor_WaitForQuiescence(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	err := proc.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	// Enqueue messages
	for i := 0; i < 20; i++ {
		gpsData := protocol.GPSData{
			Latitude:  47.674,
			Longitude: -122.121,
			Altitude:  152.4,
			Speed:     25.5,
			Heading:   180,
		}
		dataJSON, _ := json.Marshal(gpsData)

		msg := &protocol.TelemetryMessage{
			SensorID:   "gps-1",
			SensorType: "gps",
			Sequence:   uint64(i + 1),
			Timestamp:  time.Now().UTC(),
			Data:       dataJSON,
		}

		proc.Enqueue(msg)
	}

	// Wait for quiescence
	err = proc.WaitForQuiescence(5 * time.Second)
	if err != nil {
		t.Fatalf("failed waiting for quiescence: %v", err)
	}

	stats := proc.Stats()
	if stats.QueueDepth != 0 {
		t.Errorf("expected queue depth 0 after quiescence, got %d", stats.QueueDepth)
	}
}

func TestProcessor_StopNotRunning(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	// Stop without starting should not error
	err := proc.Stop()
	if err != nil {
		t.Fatalf("stop without start should not error: %v", err)
	}
}

func TestProcessor_EnqueueNotRunning(t *testing.T) {
	config := ProcessorConfig{
		NumWorkers: 2,
		QueueSize:  100,
	}
	proc := NewProcessor(config, state.NewStateStore())

	gpsData := protocol.GPSData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Speed:     25.5,
		Heading:   180,
	}
	dataJSON, _ := json.Marshal(gpsData)

	msg := &protocol.TelemetryMessage{
		SensorID:   "gps-1",
		SensorType: "gps",
		Sequence:   1,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}

	// Enqueue without starting should error
	err := proc.Enqueue(msg)
	if err == nil {
		t.Fatal("enqueue without start should error")
	}
}
