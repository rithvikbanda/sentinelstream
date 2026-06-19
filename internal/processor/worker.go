package processor

import (
	"errors"
	"log/slog"
	"sentinelstream/internal/metrics"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"sync/atomic"
	"time"
)

// WorkerStats tracks statistics for a single worker.
type WorkerStats struct {
	ID                int
	MessagesProcessed uint64
	Errors            uint64
	Anomalies         uint64
	Dropped           uint64
	Duplicates        uint64
	OutOfOrder        uint64
}

// Worker processes telemetry messages.
type Worker struct {
	id       int
	jobsChan <-chan *protocol.TelemetryMessage
	stopChan <-chan struct{}
	store    *state.StateStore

	// processingLatency and endToEndLatency are shared across the whole
	// worker pool (passed in by the Processor) so percentiles reflect
	// aggregate pipeline behavior rather than one worker in isolation.
	processingLatency *metrics.LatencyRecorder
	endToEndLatency   *metrics.LatencyRecorder

	messagesProcessed atomic.Uint64
	errors            atomic.Uint64
	anomalies         atomic.Uint64
	dropped           atomic.Uint64
	duplicates        atomic.Uint64
	outOfOrder        atomic.Uint64
}

// NewWorker creates a new worker that records sensor state in store and
// latency samples in the given shared recorders.
func NewWorker(id int, jobsChan <-chan *protocol.TelemetryMessage, stopChan <-chan struct{}, store *state.StateStore, processingLatency, endToEndLatency *metrics.LatencyRecorder) *Worker {
	return &Worker{
		id:                id,
		jobsChan:          jobsChan,
		stopChan:          stopChan,
		store:             store,
		processingLatency: processingLatency,
		endToEndLatency:   endToEndLatency,
	}
}

// Run executes the worker loop, processing messages until the channel closes or stop is signaled.
func (w *Worker) Run() {
	for {
		select {
		case <-w.stopChan:
			return
		case msg, ok := <-w.jobsChan:
			if !ok {
				// Channel closed
				return
			}
			if msg == nil {
				continue
			}

			// Process the message
			w.processMessage(msg)
		}
	}
}

// processMessage processes a single telemetry message.
func (w *Worker) processMessage(msg *protocol.TelemetryMessage) {
	start := time.Now()
	defer func() {
		w.processingLatency.Record(time.Since(start))
		w.endToEndLatency.Record(time.Since(msg.Timestamp))
	}()

	defer func() {
		if r := recover(); r != nil {
			w.errors.Add(1)
			slog.Error("worker_panic", "worker_id", w.id, "recovered", r)
		}
	}()

	// Update sensor state and detect dropped/duplicate/out-of-order sequences.
	result := w.store.Update(msg)
	if result.DroppedCount > 0 {
		w.dropped.Add(result.DroppedCount)
		slog.Warn("sequence_dropped", "sensor_id", msg.SensorID, "count", result.DroppedCount, "sequence", msg.Sequence)
	}
	if result.IsDuplicate {
		w.duplicates.Add(1)
		slog.Warn("duplicate_message", "sensor_id", msg.SensorID, "sequence", msg.Sequence)
	}
	if result.IsOutOfOrder {
		w.outOfOrder.Add(1)
		slog.Warn("out_of_order", "sensor_id", msg.SensorID, "sequence", msg.Sequence)
	}

	// Basic processing: validate sensor-specific data based on type
	var err error
	switch msg.SensorType {
	case "gps":
		_, err = protocol.ParseGPSData(msg)
	case "drone":
		_, err = protocol.ParseDroneData(msg)
	case "vehicle":
		_, err = protocol.ParseVehicleData(msg)
	case "temperature":
		_, err = protocol.ParseTemperatureSensorData(msg)
	case "radar":
		_, err = protocol.ParseRadarData(msg)
	case "patient_vitals":
		_, err = protocol.ParsePatientVitals(msg)
	default:
		// Unknown sensor type is not an error; just skip validation
	}

	if err != nil {
		// A ValidationError at this stage means the payload parsed fine but
		// a field was outside its expected range (e.g. battery > 100) - an
		// anomaly, not a processing failure. Anything else (e.g. malformed
		// payload JSON) is a genuine error.
		var verr protocol.ValidationError
		if errors.As(err, &verr) {
			w.anomalies.Add(1)
			w.store.RecordAnomaly(msg.SensorID)
			slog.Warn("anomaly_detected", "sensor_id", msg.SensorID, "error", err.Error())
		} else {
			w.errors.Add(1)
			w.store.RecordError(msg.SensorID)
			slog.Error("processing_error", "sensor_id", msg.SensorID, "error", err.Error())
		}
		return
	}

	w.messagesProcessed.Add(1)
}

// Stats returns the worker's statistics.
func (w *Worker) Stats() WorkerStats {
	return WorkerStats{
		ID:                w.id,
		MessagesProcessed: w.messagesProcessed.Load(),
		Errors:            w.errors.Load(),
		Anomalies:         w.anomalies.Load(),
		Dropped:           w.dropped.Load(),
		Duplicates:        w.duplicates.Load(),
		OutOfOrder:        w.outOfOrder.Load(),
	}
}
