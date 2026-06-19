package replay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sentinelstream/internal/protocol"
	"testing"
)

func writeEventFile(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.jsonl")

	content := ""
	for _, line := range lines {
		content += line + "\n"
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write event file: %v", err)
	}
	return path
}

func TestPlayer_ReplayCallsHandlerInOrder(t *testing.T) {
	path := writeEventFile(t,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:00Z","data":{}}`,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":2,"timestamp":"2026-01-01T00:00:01Z","data":{}}`,
		`{"sensor_id":"gps-2","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:02Z","data":{}}`,
	)

	player := NewPlayer(path)

	var seen []string
	count, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		seen = append(seen, msg.SensorID)
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 messages replayed, got %d", count)
	}

	want := []string{"gps-1", "gps-1", "gps-2"}
	for i, id := range want {
		if seen[i] != id {
			t.Errorf("position %d: expected %s, got %s", i, id, seen[i])
		}
	}
}

func TestPlayer_ReplaySkipsBlankLines(t *testing.T) {
	path := writeEventFile(t,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:00Z","data":{}}`,
		``,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":2,"timestamp":"2026-01-01T00:00:01Z","data":{}}`,
	)

	player := NewPlayer(path)
	count, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 messages replayed, got %d", count)
	}
}

func TestPlayer_ReplayFileNotFound(t *testing.T) {
	player := NewPlayer(filepath.Join(t.TempDir(), "does-not-exist.jsonl"))

	_, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestPlayer_ReplayInvalidJSONLine(t *testing.T) {
	path := writeEventFile(t,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:00Z","data":{}}`,
		`{not valid json}`,
	)

	player := NewPlayer(path)
	count, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected an error for invalid JSON")
	}
	if count != 1 {
		t.Errorf("expected 1 message successfully replayed before the bad line, got %d", count)
	}
}

func TestPlayer_ReplayHandlerError(t *testing.T) {
	path := writeEventFile(t,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:00Z","data":{}}`,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":2,"timestamp":"2026-01-01T00:00:01Z","data":{}}`,
	)

	player := NewPlayer(path)
	wantErr := errors.New("boom")

	calls := 0
	count, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		calls++
		if calls == 2 {
			return wantErr
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected an error from the handler")
	}
	if count != 1 {
		t.Errorf("expected 1 successful message before the handler failed, got %d", count)
	}
}

func TestPlayer_ReplayContextCancelled(t *testing.T) {
	path := writeEventFile(t,
		`{"sensor_id":"gps-1","sensor_type":"gps","sequence":1,"timestamp":"2026-01-01T00:00:00Z","data":{}}`,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	player := NewPlayer(path)
	count, err := player.Replay(ctx, func(msg *protocol.TelemetryMessage) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected a context-cancelled error")
	}
	if count != 0 {
		t.Errorf("expected 0 messages replayed after immediate cancellation, got %d", count)
	}
}
