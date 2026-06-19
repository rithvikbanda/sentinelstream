package state

import (
	"encoding/json"
	"sentinelstream/internal/protocol"
	"sync"
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

func TestStateStore_NewSensor(t *testing.T) {
	store := NewStateStore()

	result := store.Update(makeMsg("gps-1", 1))
	if !result.IsNewSensor {
		t.Fatal("expected IsNewSensor to be true for first message")
	}

	got, ok := store.Get("gps-1")
	if !ok {
		t.Fatal("expected sensor to be found")
	}
	if got.LastSequence != 1 {
		t.Errorf("expected LastSequence 1, got %d", got.LastSequence)
	}
	if got.MessageCount != 1 {
		t.Errorf("expected MessageCount 1, got %d", got.MessageCount)
	}
}

func TestStateStore_InOrderSequence(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	result := store.Update(makeMsg("gps-1", 2))

	if result.IsDuplicate || result.IsOutOfOrder || result.DroppedCount != 0 {
		t.Errorf("expected clean in-order update, got %+v", result)
	}

	got, _ := store.Get("gps-1")
	if got.LastSequence != 2 {
		t.Errorf("expected LastSequence 2, got %d", got.LastSequence)
	}
	if got.MessageCount != 2 {
		t.Errorf("expected MessageCount 2, got %d", got.MessageCount)
	}
}

func TestStateStore_DroppedSequence(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1041))
	store.Update(makeMsg("gps-1", 1042))
	result := store.Update(makeMsg("gps-1", 1044)) // 1043 missing

	if result.DroppedCount != 1 {
		t.Errorf("expected 1 dropped message, got %d", result.DroppedCount)
	}

	got, _ := store.Get("gps-1")
	if got.DroppedCount != 1 {
		t.Errorf("expected cumulative DroppedCount 1, got %d", got.DroppedCount)
	}
	if got.LastSequence != 1044 {
		t.Errorf("expected LastSequence 1044, got %d", got.LastSequence)
	}
}

func TestStateStore_OutOfOrderSequence(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1041))
	store.Update(makeMsg("gps-1", 1043))
	result := store.Update(makeMsg("gps-1", 1042)) // arrives late

	if !result.IsOutOfOrder {
		t.Fatal("expected message to be flagged as out of order")
	}

	got, _ := store.Get("gps-1")
	if got.OutOfOrderCount != 1 {
		t.Errorf("expected OutOfOrderCount 1, got %d", got.OutOfOrderCount)
	}
	// The high-water mark should still be the highest sequence seen, not the
	// out-of-order arrival.
	if got.LastSequence != 1043 {
		t.Errorf("expected LastSequence to remain 1043, got %d", got.LastSequence)
	}
}

func TestStateStore_DuplicateOfLatest(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	result := store.Update(makeMsg("gps-1", 1))

	if !result.IsDuplicate {
		t.Fatal("expected repeated sequence to be flagged as duplicate")
	}

	got, _ := store.Get("gps-1")
	if got.DuplicateCount != 1 {
		t.Errorf("expected DuplicateCount 1, got %d", got.DuplicateCount)
	}
}

func TestStateStore_DuplicateOfOutOfOrder(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1041))
	store.Update(makeMsg("gps-1", 1043))
	store.Update(makeMsg("gps-1", 1042))             // out of order, remembered
	result := store.Update(makeMsg("gps-1", 1042))    // same one resent

	if !result.IsDuplicate {
		t.Fatal("expected resend of a known out-of-order sequence to be a duplicate")
	}

	got, _ := store.Get("gps-1")
	if got.OutOfOrderCount != 1 {
		t.Errorf("expected OutOfOrderCount to stay at 1, got %d", got.OutOfOrderCount)
	}
	if got.DuplicateCount != 1 {
		t.Errorf("expected DuplicateCount 1, got %d", got.DuplicateCount)
	}
}

func TestStateStore_RecentSeqWindowPruning(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	// Advance far enough that sequence 1 falls outside the remembered window.
	store.Update(makeMsg("gps-1", recentSeqWindow+100))

	// Resending the now-forgotten old sequence should look like a fresh
	// out-of-order message rather than a duplicate, since it's no longer
	// remembered.
	result := store.Update(makeMsg("gps-1", 1))
	if !result.IsOutOfOrder {
		t.Errorf("expected pruned-out sequence to be treated as out of order, got %+v", result)
	}
}

func TestStateStore_RecordError(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	store.RecordError("gps-1")
	store.RecordError("unknown-sensor") // should be a no-op

	got, _ := store.Get("gps-1")
	if got.ErrorCount != 1 {
		t.Errorf("expected ErrorCount 1, got %d", got.ErrorCount)
	}
}

func TestStateStore_RecordAnomaly(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	store.RecordAnomaly("gps-1")
	store.RecordAnomaly("unknown-sensor") // should be a no-op

	got, _ := store.Get("gps-1")
	if got.AnomalyCount != 1 {
		t.Errorf("expected AnomalyCount 1, got %d", got.AnomalyCount)
	}
}

func TestStateStore_CheckStale(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))

	got, _ := store.Get("gps-1")
	if got.Status != StatusHealthy {
		t.Fatalf("expected new sensor to start healthy, got %q", got.Status)
	}

	// Not enough time has passed yet.
	if stale := store.CheckStale(time.Hour); len(stale) != 0 {
		t.Errorf("expected no stale sensors yet, got %v", stale)
	}

	// A zero timeout means "anything not seen in the last instant is stale".
	time.Sleep(time.Millisecond)
	stale := store.CheckStale(0)
	if len(stale) != 1 || stale[0] != "gps-1" {
		t.Fatalf("expected gps-1 to be newly stale, got %v", stale)
	}

	got, _ = store.Get("gps-1")
	if got.Status != StatusStale {
		t.Errorf("expected status %q, got %q", StatusStale, got.Status)
	}

	// A second scan should not report it again since it already transitioned.
	if stale := store.CheckStale(0); len(stale) != 0 {
		t.Errorf("expected no newly-stale sensors on second scan, got %v", stale)
	}

	// A fresh message should revive it.
	store.Update(makeMsg("gps-1", 2))
	got, _ = store.Get("gps-1")
	if got.Status != StatusHealthy {
		t.Errorf("expected status to revert to healthy after new data, got %q", got.Status)
	}
}

func TestStateStore_GetUnknownSensor(t *testing.T) {
	store := NewStateStore()

	_, ok := store.Get("does-not-exist")
	if ok {
		t.Fatal("expected unknown sensor lookup to return false")
	}
}

func TestStateStore_ListAndCount(t *testing.T) {
	store := NewStateStore()

	store.Update(makeMsg("gps-1", 1))
	store.Update(makeMsg("gps-2", 1))
	store.Update(makeMsg("gps-2", 2))

	if store.Count() != 2 {
		t.Errorf("expected 2 sensors, got %d", store.Count())
	}

	list := store.List()
	if len(list) != 2 {
		t.Errorf("expected list of length 2, got %d", len(list))
	}
}

func TestStateStore_ConcurrentUpdates(t *testing.T) {
	store := NewStateStore()

	const numSensors = 20
	const numMessages = 200

	var wg sync.WaitGroup
	for s := 0; s < numSensors; s++ {
		sensorID := "sensor-" + string(rune('A'+s))
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for seq := uint64(1); seq <= numMessages; seq++ {
				store.Update(makeMsg(id, seq))
			}
		}(sensorID)
	}
	wg.Wait()

	if store.Count() != numSensors {
		t.Errorf("expected %d sensors, got %d", numSensors, store.Count())
	}

	for s := 0; s < numSensors; s++ {
		sensorID := "sensor-" + string(rune('A'+s))
		got, ok := store.Get(sensorID)
		if !ok {
			t.Fatalf("expected sensor %s to exist", sensorID)
		}
		if got.MessageCount != numMessages {
			t.Errorf("sensor %s: expected MessageCount %d, got %d", sensorID, numMessages, got.MessageCount)
		}
		if got.LastSequence != numMessages {
			t.Errorf("sensor %s: expected LastSequence %d, got %d", sensorID, numMessages, got.LastSequence)
		}
	}
}
