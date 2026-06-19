package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sentinelstream/internal/metrics"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessorConfig holds configuration for the processor.
type ProcessorConfig struct {
	// NumWorkers is the number of concurrent worker goroutines.
	NumWorkers int
	// QueueSize is the capacity of the message queue (bounded channel).
	QueueSize int
}

// Processor orchestrates the worker pool and message processing pipeline.
type Processor struct {
	config    ProcessorConfig
	store     *state.StateStore
	workers   []*Worker
	jobsChan  chan *protocol.TelemetryMessage
	stopChan  chan struct{}
	wg        sync.WaitGroup
	isRunning atomic.Bool

	// processingLatency measures time spent inside a worker's processMessage
	// call; endToEndLatency measures from the message's own Timestamp (set
	// at the sensor) to the moment processing finished, capturing network
	// and queueing delay as well.
	processingLatency *metrics.LatencyRecorder
	endToEndLatency   *metrics.LatencyRecorder

	// Stats
	mu                     sync.Mutex
	totalMessagesEnqueued  atomic.Uint64
	totalMessagesProcessed atomic.Uint64
	totalErrors            atomic.Uint64
	queueFullEvents        atomic.Uint64
}

// NewProcessor creates a new processor with the given configuration. store
// holds the latest known state for every sensor and is shared with each
// worker so they can update it as messages are processed.
func NewProcessor(config ProcessorConfig, store *state.StateStore) *Processor {
	if config.NumWorkers <= 0 {
		config.NumWorkers = 4
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 1000
	}

	return &Processor{
		config:            config,
		store:             store,
		jobsChan:          make(chan *protocol.TelemetryMessage, config.QueueSize),
		stopChan:          make(chan struct{}),
		workers:           make([]*Worker, config.NumWorkers),
		processingLatency: metrics.NewLatencyRecorder(0),
		endToEndLatency:   metrics.NewLatencyRecorder(0),
	}
}

// Store returns the shared sensor state store.
func (p *Processor) Store() *state.StateStore {
	return p.store
}

// Start initializes and starts all worker goroutines.
func (p *Processor) Start(ctx context.Context) error {
	if p.isRunning.Load() {
		return fmt.Errorf("processor already running")
	}

	p.isRunning.Store(true)

	// Create and start workers
	for i := 0; i < p.config.NumWorkers; i++ {
		worker := NewWorker(i, p.jobsChan, p.stopChan, p.store, p.processingLatency, p.endToEndLatency)
		p.workers[i] = worker

		p.wg.Add(1)
		go func(w *Worker) {
			defer p.wg.Done()
			w.Run()
		}(worker)
	}

	return nil
}

// Stop gracefully stops the processor and waits for all workers to finish.
func (p *Processor) Stop() error {
	if !p.isRunning.Load() {
		return nil
	}

	p.isRunning.Store(false)

	// Close the stop channel to signal workers
	close(p.stopChan)

	// Close the jobs channel to signal no more work is coming
	close(p.jobsChan)

	// Wait for all workers to finish
	p.wg.Wait()

	return nil
}

// Enqueue submits a message to the processing pipeline.
// It blocks if the queue is full (backpressure).
func (p *Processor) Enqueue(msg *protocol.TelemetryMessage) error {
	if !p.isRunning.Load() {
		return fmt.Errorf("processor not running")
	}

	select {
	case p.jobsChan <- msg:
		p.totalMessagesEnqueued.Add(1)
		return nil
	case <-p.stopChan:
		return fmt.Errorf("processor stopping")
	}
}

// TryEnqueue attempts to submit a message without blocking.
// Returns an error if the queue is full.
func (p *Processor) TryEnqueue(msg *protocol.TelemetryMessage) error {
	if !p.isRunning.Load() {
		return fmt.Errorf("processor not running")
	}

	select {
	case p.jobsChan <- msg:
		p.totalMessagesEnqueued.Add(1)
		return nil
	default:
		p.queueFullEvents.Add(1)
		slog.Warn("queue_full", "queue_size", p.config.QueueSize, "sensor_id", msg.SensorID)
		return fmt.Errorf("queue full")
	}
}

// Stats returns aggregate statistics for the processor.
func (p *Processor) Stats() ProcessorStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	workerStats := make([]WorkerStats, len(p.workers))
	totalProcessed := uint64(0)
	totalErrors := uint64(0)
	totalAnomalies := uint64(0)
	totalDropped := uint64(0)
	totalDuplicates := uint64(0)
	totalOutOfOrder := uint64(0)

	for i, worker := range p.workers {
		stats := worker.Stats()
		workerStats[i] = stats
		totalProcessed += stats.MessagesProcessed
		totalErrors += stats.Errors
		totalAnomalies += stats.Anomalies
		totalDropped += stats.Dropped
		totalDuplicates += stats.Duplicates
		totalOutOfOrder += stats.OutOfOrder
	}

	return ProcessorStats{
		NumWorkers:             p.config.NumWorkers,
		QueueSize:              p.config.QueueSize,
		QueueDepth:             len(p.jobsChan),
		TotalMessagesEnqueued:  p.totalMessagesEnqueued.Load(),
		TotalMessagesProcessed: totalProcessed,
		TotalErrors:            totalErrors,
		TotalAnomalies:         totalAnomalies,
		TotalDropped:           totalDropped,
		TotalDuplicates:        totalDuplicates,
		TotalOutOfOrder:        totalOutOfOrder,
		KnownSensors:           p.store.Count(),
		QueueFullEvents:        p.queueFullEvents.Load(),
		IsRunning:              p.isRunning.Load(),
		WorkerStats:            workerStats,
		ProcessingLatency:      p.processingLatency.Snapshot(),
		EndToEndLatency:        p.endToEndLatency.Snapshot(),
	}
}

// ProcessorStats holds aggregate statistics for the processor.
type ProcessorStats struct {
	NumWorkers             int
	QueueSize              int
	QueueDepth             int
	TotalMessagesEnqueued  uint64
	TotalMessagesProcessed uint64
	TotalErrors            uint64
	TotalAnomalies         uint64
	TotalDropped           uint64
	TotalDuplicates        uint64
	TotalOutOfOrder        uint64
	KnownSensors           int
	QueueFullEvents        uint64
	IsRunning              bool
	WorkerStats            []WorkerStats
	ProcessingLatency      metrics.Snapshot
	EndToEndLatency        metrics.Snapshot
}

// String returns a formatted string representation of the stats.
func (s ProcessorStats) String() string {
	return fmt.Sprintf(
		"Processor: %d workers, queue %d/%d, enqueued=%d, processed=%d, errors=%d, anomalies=%d, dropped=%d, duplicates=%d, out_of_order=%d, sensors=%d, queue_full=%d, e2e_latency(p50/p95/p99)=%s/%s/%s",
		s.NumWorkers,
		s.QueueDepth,
		s.QueueSize,
		s.TotalMessagesEnqueued,
		s.TotalMessagesProcessed,
		s.TotalErrors,
		s.TotalAnomalies,
		s.TotalDropped,
		s.TotalDuplicates,
		s.TotalOutOfOrder,
		s.KnownSensors,
		s.QueueFullEvents,
		s.EndToEndLatency.P50,
		s.EndToEndLatency.P95,
		s.EndToEndLatency.P99,
	)
}

// WaitForQuiescence waits until the processing queue is empty.
// Timeout specifies the maximum time to wait.
func (p *Processor) WaitForQuiescence(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for {
		if !p.isRunning.Load() {
			return fmt.Errorf("processor not running")
		}

		if len(p.jobsChan) == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for queue to empty")
		}

		time.Sleep(10 * time.Millisecond)
	}
}
