package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestInit_EmitsExpectedJSONKeys(t *testing.T) {
	var buf bytes.Buffer
	logger := Init(&buf)

	logger.Warn("sensor_stale", "sensor_id", "drone-17")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log line as JSON: %v (line: %s)", err, buf.String())
	}

	if entry["event"] != "sensor_stale" {
		t.Errorf("expected event %q, got %v", "sensor_stale", entry["event"])
	}
	if entry["sensor_id"] != "drone-17" {
		t.Errorf("expected sensor_id %q, got %v", "drone-17", entry["sensor_id"])
	}
	if _, ok := entry["timestamp"]; !ok {
		t.Error("expected a timestamp field")
	}
	if _, ok := entry["time"]; ok {
		t.Error("expected the standard slog 'time' key to be renamed away")
	}
	if _, ok := entry["msg"]; ok {
		t.Error("expected the standard slog 'msg' key to be renamed away")
	}
	if entry["level"] != "WARN" {
		t.Errorf("expected level WARN, got %v", entry["level"])
	}
}

func TestInit_SetsDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	Init(&buf)

	slog.Warn("test_event")

	if buf.Len() == 0 {
		t.Error("expected slog.Default() to write through the configured handler")
	}
}
