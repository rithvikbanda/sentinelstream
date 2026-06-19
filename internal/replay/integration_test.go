package replay

import (
	"context"
	"path/filepath"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"testing"
	"time"
)

// TestReplayProducesExpectedState records a stream of messages (including a
// sequence gap and a sensor-state-relevant pattern), replays them through a
// real processor + state store, and verifies the resulting sensor state
// matches what direct processing of the original stream would have
// produced.
func TestReplayProducesExpectedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")

	rec, err := NewEventRecorder(path)
	if err != nil {
		t.Fatalf("failed to create recorder: %v", err)
	}

	if err := rec.Record(makeMsg("gps-1", 1)); err != nil {
		t.Fatalf("failed to record: %v", err)
	}
	if err := rec.Record(makeMsg("gps-1", 2)); err != nil {
		t.Fatalf("failed to record: %v", err)
	}
	if err := rec.Record(makeMsg("gps-1", 4)); err != nil { // sequence 3 missing -> 1 dropped
		t.Fatalf("failed to record: %v", err)
	}
	if err := rec.Record(makeMsg("gps-2", 1)); err != nil {
		t.Fatalf("failed to record: %v", err)
	}
	if err := rec.Close(); err != nil {
		t.Fatalf("failed to close recorder: %v", err)
	}

	store := state.NewStateStore()
	// A single worker keeps processing order identical to enqueue order, so
	// the resulting sensor state is deterministic and matches what
	// processing the original stream in real time would have produced.
	proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: 1, QueueSize: 100}, store)
	if err := proc.Start(context.Background()); err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	player := NewPlayer(path)
	count, err := player.Replay(context.Background(), func(msg *protocol.TelemetryMessage) error {
		return proc.Enqueue(msg)
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if count != 4 {
		t.Fatalf("expected 4 messages replayed, got %d", count)
	}

	if err := proc.WaitForQuiescence(5 * time.Second); err != nil {
		t.Fatalf("processor never drained: %v", err)
	}

	gps1, ok := store.Get("gps-1")
	if !ok {
		t.Fatal("expected gps-1 to be known after replay")
	}
	if gps1.MessageCount != 3 {
		t.Errorf("expected gps-1 MessageCount 3, got %d", gps1.MessageCount)
	}
	if gps1.LastSequence != 4 {
		t.Errorf("expected gps-1 LastSequence 4, got %d", gps1.LastSequence)
	}
	if gps1.DroppedCount != 1 {
		t.Errorf("expected gps-1 DroppedCount 1, got %d", gps1.DroppedCount)
	}

	gps2, ok := store.Get("gps-2")
	if !ok {
		t.Fatal("expected gps-2 to be known after replay")
	}
	if gps2.MessageCount != 1 {
		t.Errorf("expected gps-2 MessageCount 1, got %d", gps2.MessageCount)
	}

	if store.Count() != 2 {
		t.Errorf("expected 2 known sensors after replay, got %d", store.Count())
	}
}
