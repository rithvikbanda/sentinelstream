// Package health runs a background scan that marks sensors as stale once
// they stop sending telemetry.
package health

import (
	"context"
	"log/slog"
	"sentinelstream/internal/state"
	"sync"
	"time"
)

// MonitorConfig configures the stale-sensor monitor.
type MonitorConfig struct {
	// StaleTimeout is how long a sensor can go without a message before
	// it's marked stale.
	StaleTimeout time.Duration
	// CheckInterval is how often the monitor scans for stale sensors.
	CheckInterval time.Duration
}

// Monitor periodically scans a state.StateStore for sensors that have
// stopped sending data and marks them stale.
type Monitor struct {
	config   MonitorConfig
	store    *state.StateStore
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewMonitor creates a stale-sensor monitor for store.
func NewMonitor(config MonitorConfig, store *state.StateStore) *Monitor {
	if config.StaleTimeout <= 0 {
		config.StaleTimeout = 5 * time.Second
	}
	if config.CheckInterval <= 0 {
		config.CheckInterval = 1 * time.Second
	}

	return &Monitor{
		config:   config,
		store:    store,
		stopChan: make(chan struct{}),
	}
}

// Start begins the periodic stale-sensor scan in a background goroutine.
func (m *Monitor) Start(ctx context.Context) {
	m.wg.Add(1)
	go m.run(ctx)
}

// Stop halts the monitor and waits for the background goroutine to exit.
func (m *Monitor) Stop() {
	close(m.stopChan)
	m.wg.Wait()
}

func (m *Monitor) run(ctx context.Context) {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopChan:
			return
		case <-ticker.C:
			for _, sensorID := range m.store.CheckStale(m.config.StaleTimeout) {
				slog.Warn("sensor_stale", "sensor_id", sensorID, "timeout_ms", m.config.StaleTimeout.Milliseconds())
			}
		}
	}
}
