package health

import (
	"context"
	"encoding/json"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
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

func TestMonitor_MarksStaleSensor(t *testing.T) {
	store := state.NewStateStore()
	store.Update(makeMsg("gps-1", 1))

	monitor := NewMonitor(MonitorConfig{
		StaleTimeout:  20 * time.Millisecond,
		CheckInterval: 10 * time.Millisecond,
	}, store)

	monitor.Start(context.Background())
	defer monitor.Stop()

	deadline := time.After(2 * time.Second)
	for {
		got, _ := store.Get("gps-1")
		if got.Status == state.StatusStale {
			break
		}
		select {
		case <-deadline:
			t.Fatal("expected sensor to be marked stale")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestMonitor_HealthySensorStaysHealthy(t *testing.T) {
	store := state.NewStateStore()
	store.Update(makeMsg("gps-1", 1))

	monitor := NewMonitor(MonitorConfig{
		StaleTimeout:  time.Hour,
		CheckInterval: 10 * time.Millisecond,
	}, store)

	monitor.Start(context.Background())
	defer monitor.Stop()

	time.Sleep(50 * time.Millisecond)

	got, _ := store.Get("gps-1")
	if got.Status != state.StatusHealthy {
		t.Errorf("expected sensor to remain healthy, got %q", got.Status)
	}
}

func TestMonitor_StopHaltsScanning(t *testing.T) {
	store := state.NewStateStore()
	store.Update(makeMsg("gps-1", 1))

	monitor := NewMonitor(MonitorConfig{
		StaleTimeout:  20 * time.Millisecond,
		CheckInterval: 10 * time.Millisecond,
	}, store)

	monitor.Start(context.Background())
	monitor.Stop() // stop before the sensor has a chance to go stale

	time.Sleep(50 * time.Millisecond)

	got, _ := store.Get("gps-1")
	if got.Status != state.StatusHealthy {
		t.Errorf("expected sensor to remain healthy after monitor stopped, got %q", got.Status)
	}
}

func TestNewMonitor_Defaults(t *testing.T) {
	store := state.NewStateStore()
	monitor := NewMonitor(MonitorConfig{}, store)

	if monitor.config.StaleTimeout != 5*time.Second {
		t.Errorf("expected default StaleTimeout of 5s, got %s", monitor.config.StaleTimeout)
	}
	if monitor.config.CheckInterval != 1*time.Second {
		t.Errorf("expected default CheckInterval of 1s, got %s", monitor.config.CheckInterval)
	}
}
