// Package replay records accepted telemetry messages to a JSON Lines file
// and can later replay them back through the same processing pipeline -
// useful for reproducing bugs, regression testing, and benchmarking.
package replay

import (
	"encoding/json"
	"fmt"
	"os"
	"sentinelstream/internal/protocol"
	"sync"
)

// EventRecorder appends telemetry messages to a JSON Lines file, one
// message per line.
type EventRecorder struct {
	mu   sync.Mutex
	file *os.File
}

// NewEventRecorder opens (creating if necessary, appending if it already
// exists) the event file at path.
func NewEventRecorder(path string) (*EventRecorder, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open event file %s: %w", path, err)
	}
	return &EventRecorder{file: f}, nil
}

// Record appends msg to the event file as a single JSON line.
func (r *EventRecorder) Record(msg *protocol.TelemetryMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	data = append(data, '\n')

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, err := r.file.Write(data); err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}
	return nil
}

// Close closes the underlying event file.
func (r *EventRecorder) Close() error {
	return r.file.Close()
}
