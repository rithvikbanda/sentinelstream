// Package state provides a thread-safe in-memory store of the latest known
// state for every sensor, including sequence-number tracking used to detect
// dropped, duplicate, and out-of-order messages.
package state

import (
	"sentinelstream/internal/protocol"
	"sync"
	"time"
)

// recentSeqWindow bounds how far behind the high-water mark a sequence
// number is still remembered for duplicate detection. Without this, a
// long-running sensor would grow its seen-sequence set without limit.
const recentSeqWindow = 64

// Sensor health statuses.
const (
	StatusHealthy = "healthy"
	StatusStale   = "stale"
)

// SensorState is a snapshot of the latest known state for a single sensor.
type SensorState struct {
	SensorID        string
	SensorType      string
	Status          string
	LastSequence    uint64
	FirstSeen       time.Time
	LastSeen        time.Time
	MessageCount    uint64
	DroppedCount    uint64
	DuplicateCount  uint64
	OutOfOrderCount uint64
	ErrorCount      uint64
	AnomalyCount    uint64
}

// UpdateResult describes what was detected while applying a message to the
// store, so callers (e.g. workers) can log or count events without
// re-deriving the sequence logic themselves.
type UpdateResult struct {
	IsNewSensor  bool
	IsDuplicate  bool
	IsOutOfOrder bool
	// DroppedCount is the number of sequence numbers presumed dropped
	// because this message's sequence jumped ahead of the expected next one.
	DroppedCount uint64
}

// sensorEntry holds the exported state plus the bookkeeping needed to
// distinguish a duplicate from a genuinely new out-of-order message.
type sensorEntry struct {
	state SensorState
	// recentSeqs remembers sequence numbers within recentSeqWindow of the
	// high-water mark, so a repeated out-of-order message can be told apart
	// from one that's merely arriving late for the first time.
	recentSeqs map[uint64]struct{}
}

// StateStore tracks the latest state of every sensor seen by the system.
type StateStore struct {
	mu      sync.RWMutex
	sensors map[string]*sensorEntry
}

// NewStateStore creates an empty state store.
func NewStateStore() *StateStore {
	return &StateStore{
		sensors: make(map[string]*sensorEntry),
	}
}

// Update applies a successfully parsed telemetry message to the store,
// updating sequence tracking and counters for its sensor. It returns what
// was detected about the message's sequence number.
func (s *StateStore) Update(msg *protocol.TelemetryMessage) UpdateResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	entry, exists := s.sensors[msg.SensorID]
	if !exists {
		s.sensors[msg.SensorID] = &sensorEntry{
			state: SensorState{
				SensorID:     msg.SensorID,
				SensorType:   msg.SensorType,
				Status:       StatusHealthy,
				LastSequence: msg.Sequence,
				FirstSeen:    now,
				LastSeen:     now,
				MessageCount: 1,
			},
			recentSeqs: map[uint64]struct{}{msg.Sequence: {}},
		}
		return UpdateResult{IsNewSensor: true}
	}

	st := &entry.state
	st.LastSeen = now
	st.Status = StatusHealthy // any fresh message revives a previously stale sensor
	st.MessageCount++

	result := UpdateResult{}

	switch {
	case msg.Sequence == st.LastSequence:
		result.IsDuplicate = true
		st.DuplicateCount++

	case msg.Sequence > st.LastSequence:
		if gap := msg.Sequence - st.LastSequence - 1; gap > 0 {
			st.DroppedCount += gap
			result.DroppedCount = gap
		}
		st.LastSequence = msg.Sequence
		entry.recentSeqs[msg.Sequence] = struct{}{}
		pruneRecentSeqs(entry)

	default: // msg.Sequence < st.LastSequence
		if _, seen := entry.recentSeqs[msg.Sequence]; seen {
			result.IsDuplicate = true
			st.DuplicateCount++
		} else {
			result.IsOutOfOrder = true
			st.OutOfOrderCount++
			entry.recentSeqs[msg.Sequence] = struct{}{}
		}
	}

	return result
}

// pruneRecentSeqs discards remembered sequence numbers that have fallen far
// enough behind the high-water mark that they can no longer be confused
// with a freshly out-of-order message.
func pruneRecentSeqs(entry *sensorEntry) {
	if entry.state.LastSequence < recentSeqWindow {
		return
	}
	floor := entry.state.LastSequence - recentSeqWindow
	for seq := range entry.recentSeqs {
		if seq < floor {
			delete(entry.recentSeqs, seq)
		}
	}
}

// RecordError increments the error count for a sensor that produced a
// message which failed downstream (e.g. sensor-specific) validation. It is
// a no-op if the sensor is unknown.
func (s *StateStore) RecordError(sensorID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.sensors[sensorID]; ok {
		entry.state.ErrorCount++
	}
}

// RecordAnomaly increments the anomaly count for a sensor that sent a
// well-formed message with a value outside its expected range (e.g. battery
// above 100%). It is a no-op if the sensor is unknown.
func (s *StateStore) RecordAnomaly(sensorID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.sensors[sensorID]; ok {
		entry.state.AnomalyCount++
	}
}

// CheckStale marks any sensor that hasn't been seen within timeout as
// stale, and returns the IDs of sensors that just transitioned from healthy
// to stale (so callers can log a "sensor_stale" event exactly once per
// transition, not on every scan).
func (s *StateStore) CheckStale(timeout time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	var newlyStale []string
	for _, entry := range s.sensors {
		if entry.state.Status == StatusStale {
			continue
		}
		if now.Sub(entry.state.LastSeen) > timeout {
			entry.state.Status = StatusStale
			newlyStale = append(newlyStale, entry.state.SensorID)
		}
	}
	return newlyStale
}

// Get returns a copy of the current state for a sensor, if known.
func (s *StateStore) Get(sensorID string) (SensorState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.sensors[sensorID]
	if !ok {
		return SensorState{}, false
	}
	return entry.state, true
}

// List returns a copy of the current state for every known sensor.
func (s *StateStore) List() []SensorState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]SensorState, 0, len(s.sensors))
	for _, entry := range s.sensors {
		out = append(out, entry.state)
	}
	return out
}

// Count returns the number of distinct sensors known to the store.
func (s *StateStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sensors)
}
