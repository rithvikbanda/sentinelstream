// Package api exposes sensor state, stream health, and processing metrics
// over a small REST API.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sentinelstream/internal/ingestion"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/state"
	"time"
)

// Server exposes the telemetry server's state over HTTP.
type Server struct {
	httpServer *http.Server
	listener   net.Listener

	store *state.StateStore
	proc  *processor.Processor
	udp   *ingestion.UDPListener
	tcp   *ingestion.TCPListener
}

// NewServer creates an API server bound to addr (e.g. ":8080"), backed by
// the given sensor store, processor, and protocol listeners.
func NewServer(addr string, store *state.StateStore, proc *processor.Processor, udp *ingestion.UDPListener, tcp *ingestion.TCPListener) *Server {
	s := &Server{
		store: store,
		proc:  proc,
		udp:   udp,
		tcp:   tcp,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sensors", s.handleListSensors)
	mux.HandleFunc("GET /api/v1/sensors/{sensor_id}", s.handleGetSensor)
	mux.HandleFunc("GET /api/v1/health/streams", s.handleHealthStreams)
	mux.HandleFunc("GET /api/v1/metrics/summary", s.handleMetricsSummary)

	s.httpServer = &http.Server{Addr: addr, Handler: withCORS(mux)}
	return s
}

// withCORS allows any origin to read from this read-only, local-demo API,
// so a separately-served dashboard (e.g. on a Node dev server) can call it
// directly from the browser without a proxy.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Start binds the configured address and begins serving HTTP requests in a
// background goroutine. A bind failure is returned immediately.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.httpServer.Addr, err)
	}
	s.listener = ln

	go func() {
		if err := s.httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[API] server error: %v\n", err)
		}
	}()

	return nil
}

// Addr returns the address the server is actually listening on, useful when
// the configured port was 0.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Stop gracefully shuts down the HTTP server, waiting for in-flight
// requests to finish or ctx to expire.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}

// sensorSummary is the list-endpoint representation of a sensor.
type sensorSummary struct {
	SensorID   string    `json:"sensor_id"`
	SensorType string    `json:"sensor_type"`
	Status     string    `json:"status"`
	LastSeen   time.Time `json:"last_seen"`
}

// sensorDetail is the single-sensor representation, including counters.
type sensorDetail struct {
	SensorID         string    `json:"sensor_id"`
	SensorType       string    `json:"sensor_type"`
	Status           string    `json:"status"`
	LastSequence     uint64    `json:"last_sequence"`
	MessagesReceived uint64    `json:"messages_received"`
	MessagesDropped  uint64    `json:"messages_dropped"`
	Duplicates       uint64    `json:"duplicates"`
	OutOfOrder       uint64    `json:"out_of_order"`
	Errors           uint64    `json:"errors"`
	Anomalies        uint64    `json:"anomalies"`
	LastSeen         time.Time `json:"last_seen"`
}

func toSensorSummary(sn state.SensorState) sensorSummary {
	return sensorSummary{
		SensorID:   sn.SensorID,
		SensorType: sn.SensorType,
		Status:     sn.Status,
		LastSeen:   sn.LastSeen,
	}
}

func toSensorDetail(sn state.SensorState) sensorDetail {
	return sensorDetail{
		SensorID:         sn.SensorID,
		SensorType:       sn.SensorType,
		Status:           sn.Status,
		LastSequence:     sn.LastSequence,
		MessagesReceived: sn.MessageCount,
		MessagesDropped:  sn.DroppedCount,
		Duplicates:       sn.DuplicateCount,
		OutOfOrder:       sn.OutOfOrderCount,
		Errors:           sn.ErrorCount,
		Anomalies:        sn.AnomalyCount,
		LastSeen:         sn.LastSeen,
	}
}

func (s *Server) handleListSensors(w http.ResponseWriter, r *http.Request) {
	sensors := s.store.List()
	summaries := make([]sensorSummary, 0, len(sensors))
	for _, sn := range sensors {
		summaries = append(summaries, toSensorSummary(sn))
	}
	writeJSON(w, http.StatusOK, summaries)
}

func (s *Server) handleGetSensor(w http.ResponseWriter, r *http.Request) {
	sensorID := r.PathValue("sensor_id")

	sn, ok := s.store.Get(sensorID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, fmt.Sprintf("sensor %q not found", sensorID))
		return
	}

	writeJSON(w, http.StatusOK, toSensorDetail(sn))
}

// listenerHealth is the JSON shape for a single protocol listener's health.
type listenerHealth struct {
	MessagesReceived uint64 `json:"messages_received"`
	MessagesRejected uint64 `json:"messages_rejected"`
	IsRunning        bool   `json:"is_running"`
}

// tcpListenerHealth additionally reports connection counts.
type tcpListenerHealth struct {
	MessagesReceived  uint64 `json:"messages_received"`
	MessagesRejected  uint64 `json:"messages_rejected"`
	ActiveConnections int64  `json:"active_connections"`
	TotalConnections  uint64 `json:"total_connections"`
	IsRunning         bool   `json:"is_running"`
}

type streamHealth struct {
	UDP            listenerHealth    `json:"udp"`
	TCP            tcpListenerHealth `json:"tcp"`
	QueueDepth     int               `json:"queue_depth"`
	QueueCapacity  int               `json:"queue_capacity"`
	WorkersRunning bool              `json:"workers_running"`
}

func (s *Server) handleHealthStreams(w http.ResponseWriter, r *http.Request) {
	udpStats := s.udp.Stats()
	tcpStats := s.tcp.Stats()
	procStats := s.proc.Stats()

	writeJSON(w, http.StatusOK, streamHealth{
		UDP: listenerHealth{
			MessagesReceived: udpStats.MessagesReceived,
			MessagesRejected: udpStats.MessagesRejected,
			IsRunning:        udpStats.IsRunning,
		},
		TCP: tcpListenerHealth{
			MessagesReceived:  tcpStats.MessagesReceived,
			MessagesRejected:  tcpStats.MessagesRejected,
			ActiveConnections: tcpStats.ActiveConnections,
			TotalConnections:  tcpStats.TotalConnections,
			IsRunning:         tcpStats.IsRunning,
		},
		QueueDepth:     procStats.QueueDepth,
		QueueCapacity:  procStats.QueueSize,
		WorkersRunning: procStats.IsRunning,
	})
}

// metricsSummary is the JSON shape for the system-wide metrics endpoint.
type metricsSummary struct {
	MessagesReceived     uint64 `json:"messages_received"`
	MessagesProcessed    uint64 `json:"messages_processed"`
	ValidationErrors     uint64 `json:"validation_errors"`
	ProcessingErrors     uint64 `json:"processing_errors"`
	AnomaliesDetected    uint64 `json:"anomalies_detected"`
	DroppedMessages      uint64 `json:"dropped_messages"`
	DuplicateMessages    uint64 `json:"duplicate_messages"`
	OutOfOrderMessages   uint64 `json:"out_of_order_messages"`
	QueueDepth           int    `json:"queue_depth"`
	ActiveTCPConnections int64  `json:"active_tcp_connections"`
	HealthySensors       int    `json:"healthy_sensors"`
	StaleSensors         int    `json:"stale_sensors"`
}

func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	udpStats := s.udp.Stats()
	tcpStats := s.tcp.Stats()
	procStats := s.proc.Stats()

	healthy, stale := 0, 0
	for _, sn := range s.store.List() {
		switch sn.Status {
		case state.StatusHealthy:
			healthy++
		case state.StatusStale:
			stale++
		}
	}

	writeJSON(w, http.StatusOK, metricsSummary{
		MessagesReceived:     udpStats.MessagesReceived + tcpStats.MessagesReceived,
		MessagesProcessed:    procStats.TotalMessagesProcessed,
		ValidationErrors:     udpStats.MessagesRejected + tcpStats.MessagesRejected,
		ProcessingErrors:     procStats.TotalErrors,
		AnomaliesDetected:    procStats.TotalAnomalies,
		DroppedMessages:      procStats.TotalDropped,
		DuplicateMessages:    procStats.TotalDuplicates,
		OutOfOrderMessages:   procStats.TotalOutOfOrder,
		QueueDepth:           procStats.QueueDepth,
		ActiveTCPConnections: tcpStats.ActiveConnections,
		HealthySensors:       healthy,
		StaleSensors:         stale,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
