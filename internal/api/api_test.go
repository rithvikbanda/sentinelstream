package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sentinelstream/internal/ingestion"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"testing"
	"time"
)

// newTestServer wires up a Server backed by real (but unstarted, port-0)
// listeners and a started processor, so handlers can be exercised directly
// without binding real sockets for every test.
func newTestServer(t *testing.T) (*Server, *state.StateStore) {
	t.Helper()

	store := state.NewStateStore()
	proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: 2, QueueSize: 10}, store)
	if err := proc.Start(context.Background()); err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	t.Cleanup(func() { proc.Stop() })

	udp := ingestion.NewUDPListener(0, 10)
	tcp := ingestion.NewTCPListener(0, 10)

	srv := NewServer(":0", store, proc, udp, tcp)
	return srv, store
}

func doRequest(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)
	return rec
}

func TestCORS_AllowsCrossOriginGet(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/sensors")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("expected Access-Control-Allow-Origin: *, got %q", got)
	}
}

func TestCORS_HandlesPreflightOptions(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodOptions, "/api/v1/sensors")
	if rec.Code != http.StatusNoContent {
		t.Errorf("expected 204 for OPTIONS preflight, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("expected Access-Control-Allow-Origin: *, got %q", got)
	}
}

func TestHandleListSensors_Empty(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/sensors")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var sensors []sensorSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &sensors); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(sensors) != 0 {
		t.Errorf("expected 0 sensors, got %d", len(sensors))
	}
}

func TestHandleListSensors_WithData(t *testing.T) {
	srv, store := newTestServer(t)

	store.Update(makeMsg("gps-1", 1))
	store.Update(makeMsg("gps-2", 1))

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/sensors")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var sensors []sensorSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &sensors); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(sensors) != 2 {
		t.Fatalf("expected 2 sensors, got %d", len(sensors))
	}
	for _, sn := range sensors {
		if sn.Status != state.StatusHealthy {
			t.Errorf("expected sensor %s to be healthy, got %q", sn.SensorID, sn.Status)
		}
	}
}

func TestHandleGetSensor_Found(t *testing.T) {
	srv, store := newTestServer(t)

	store.Update(makeMsg("gps-1", 1041))
	store.Update(makeMsg("gps-1", 1042))
	store.Update(makeMsg("gps-1", 1044)) // gap -> 1 dropped

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/sensors/gps-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var detail sensorDetail
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if detail.SensorID != "gps-1" {
		t.Errorf("expected sensor_id gps-1, got %s", detail.SensorID)
	}
	if detail.LastSequence != 1044 {
		t.Errorf("expected last_sequence 1044, got %d", detail.LastSequence)
	}
	if detail.MessagesReceived != 3 {
		t.Errorf("expected messages_received 3, got %d", detail.MessagesReceived)
	}
	if detail.MessagesDropped != 1 {
		t.Errorf("expected messages_dropped 1, got %d", detail.MessagesDropped)
	}
}

func TestHandleGetSensor_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/sensors/does-not-exist")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected a non-empty error message")
	}
}

func TestHandleSensors_MethodNotAllowed(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodPost, "/api/v1/sensors")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestHandleUnknownPath_NotFound(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestHandleHealthStreams(t *testing.T) {
	srv, _ := newTestServer(t)

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/health/streams")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var health streamHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &health); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if health.QueueCapacity != 10 {
		t.Errorf("expected queue_capacity 10, got %d", health.QueueCapacity)
	}
	if !health.WorkersRunning {
		t.Error("expected workers_running true")
	}
}

func TestHandleMetricsSummary(t *testing.T) {
	srv, store := newTestServer(t)

	store.Update(makeMsg("gps-1", 1))
	store.Update(makeMsg("gps-2", 1))
	time.Sleep(time.Millisecond) // ensure LastSeen is strictly in the past
	// Force both sensors stale without waiting on a real timeout.
	store.CheckStale(0)
	// Reviving gps-1 keeps it healthy for the assertion below.
	store.Update(makeMsg("gps-1", 2))

	rec := doRequest(t, srv, http.MethodGet, "/api/v1/metrics/summary")
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var summary metricsSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &summary); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if summary.HealthySensors != 1 {
		t.Errorf("expected 1 healthy sensor, got %d", summary.HealthySensors)
	}
	if summary.StaleSensors != 1 {
		t.Errorf("expected 1 stale sensor, got %d", summary.StaleSensors)
	}
}

func TestServer_StartStopServesRequests(t *testing.T) {
	store := state.NewStateStore()
	proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: 1, QueueSize: 10}, store)
	if err := proc.Start(context.Background()); err != nil {
		t.Fatalf("failed to start processor: %v", err)
	}
	defer proc.Stop()

	udp := ingestion.NewUDPListener(0, 10)
	tcp := ingestion.NewTCPListener(0, 10)

	srv := NewServer("127.0.0.1:0", store, proc, udp, tcp)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start API server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Stop(ctx)
	}()

	resp, err := http.Get("http://" + srv.Addr() + "/api/v1/sensors")
	if err != nil {
		t.Fatalf("failed to GET sensors: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

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
