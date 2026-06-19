package replay

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sentinelstream/internal/protocol"
	"testing"
	"time"
)

func makeMsg(sensorID string, seq uint64) *protocol.TelemetryMessage {
	data, _ := json.Marshal(protocol.GPSData{Latitude: 1, Longitude: 1})
	return &protocol.TelemetryMessage{
		SensorID:   sensorID,
		SensorType: "gps",
		Sequence:   seq,
		Timestamp:  time.Now().UTC(),
		Data:       data,
	}
}

func TestEventRecorder_RecordWritesOneLinePerMessage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	rec, err := NewEventRecorder(path)
	if err != nil {
		t.Fatalf("failed to create recorder: %v", err)
	}

	for i := uint64(1); i <= 3; i++ {
		if err := rec.Record(makeMsg("gps-1", i)); err != nil {
			t.Fatalf("failed to record message %d: %v", i, err)
		}
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("failed to close recorder: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open event file: %v", err)
	}
	defer f.Close()

	lineCount := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var msg protocol.TelemetryMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			t.Fatalf("line %d is not valid JSON: %v", lineCount+1, err)
		}
		lineCount++
	}
	if lineCount != 3 {
		t.Errorf("expected 3 lines, got %d", lineCount)
	}
}

func TestEventRecorder_AppendsToExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	rec1, err := NewEventRecorder(path)
	if err != nil {
		t.Fatalf("failed to create recorder: %v", err)
	}
	rec1.Record(makeMsg("gps-1", 1))
	rec1.Close()

	rec2, err := NewEventRecorder(path)
	if err != nil {
		t.Fatalf("failed to reopen recorder: %v", err)
	}
	rec2.Record(makeMsg("gps-1", 2))
	rec2.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read event file: %v", err)
	}

	lines := 0
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		lines++
	}
	if lines != 2 {
		t.Errorf("expected 2 lines after reopening and appending, got %d", lines)
	}
}
