package ingestion

import (
	"fmt"
	"log/slog"
	"net"
	"sentinelstream/internal/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// UDPListener listens for telemetry messages on a UDP port.
type UDPListener struct {
	port              int
	maxMessageSize    int
	conn              *net.UDPConn
	messagesChan      chan *protocol.TelemetryMessage
	errorsChan        chan error
	stopChan          chan struct{}
	wg                sync.WaitGroup
	isRunning         atomic.Bool
	messagesReceived  atomic.Uint64
	messagesRejected  atomic.Uint64
	lastErrorTime     time.Time
	lastErrorMessage  string
	mu                sync.Mutex
}

// NewUDPListener creates a new UDP listener on the specified port.
func NewUDPListener(port int, bufferSize int) *UDPListener {
	return &UDPListener{
		port:           port,
		maxMessageSize: 65507, // Max UDP datagram payload
		messagesChan:   make(chan *protocol.TelemetryMessage, bufferSize),
		errorsChan:     make(chan error, 10),
		stopChan:       make(chan struct{}),
	}
}

// Start begins listening for UDP messages on the configured port.
func (l *UDPListener) Start() error {
	if l.isRunning.Load() {
		return fmt.Errorf("listener already running")
	}

	addr := net.UDPAddr{
		Port: l.port,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return fmt.Errorf("failed to listen on UDP %d: %w", l.port, err)
	}

	l.conn = conn
	l.isRunning.Store(true)

	l.wg.Add(1)
	go l.receiveLoop()

	return nil
}

// Stop gracefully stops the listener.
func (l *UDPListener) Stop() error {
	if !l.isRunning.Load() {
		return nil
	}

	l.isRunning.Store(false)
	close(l.stopChan)

	if l.conn != nil {
		l.conn.Close()
	}

	l.wg.Wait()
	close(l.messagesChan)
	close(l.errorsChan)

	return nil
}

// Messages returns the channel for receiving valid telemetry messages.
func (l *UDPListener) Messages() <-chan *protocol.TelemetryMessage {
	return l.messagesChan
}

// Errors returns the channel for receiving parsing/validation errors.
func (l *UDPListener) Errors() <-chan error {
	return l.errorsChan
}

// receiveLoop reads UDP datagrams and processes them.
func (l *UDPListener) receiveLoop() {
	defer l.wg.Done()

	buffer := make([]byte, l.maxMessageSize)

	for {
		select {
		case <-l.stopChan:
			return
		default:
		}

		n, remoteAddr, err := l.conn.ReadFromUDP(buffer)
		if err != nil {
			// Check if listener was stopped
			if !l.isRunning.Load() {
				return
			}

			l.mu.Lock()
			l.lastErrorTime = time.Now()
			l.lastErrorMessage = fmt.Sprintf("read error from %s: %v", remoteAddr, err)
			l.mu.Unlock()

			select {
			case l.errorsChan <- err:
			case <-l.stopChan:
				return
			}
			continue
		}

		if n == 0 {
			continue
		}

		// Parse the JSON message
		msg, err := protocol.ParseTelemetryMessage(buffer[:n])
		if err != nil {
			l.messagesRejected.Add(1)

			l.mu.Lock()
			l.lastErrorTime = time.Now()
			l.lastErrorMessage = fmt.Sprintf("parse error from %s: %v", remoteAddr, err)
			l.mu.Unlock()

			slog.Warn("validation_failed", "protocol", "udp", "remote_addr", remoteAddr.String(), "error", err.Error())

			select {
			case l.errorsChan <- fmt.Errorf("from %s: %w", remoteAddr, err):
			case <-l.stopChan:
				return
			}
			continue
		}

		// Successfully parsed and validated
		l.messagesReceived.Add(1)

		select {
		case l.messagesChan <- msg:
			// Message sent successfully
		case <-l.stopChan:
			return
		}
	}
}

// Stats returns current listener statistics.
func (l *UDPListener) Stats() ListenerStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	return ListenerStats{
		MessagesReceived: l.messagesReceived.Load(),
		MessagesRejected: l.messagesRejected.Load(),
		LastErrorTime:    l.lastErrorTime,
		LastErrorMessage: l.lastErrorMessage,
		IsRunning:        l.isRunning.Load(),
	}
}

// ListenerStats holds statistics about the listener.
type ListenerStats struct {
	MessagesReceived  uint64
	MessagesRejected  uint64
	LastErrorTime     time.Time
	LastErrorMessage  string
	IsRunning         bool
}

// String returns a formatted string representation of stats.
func (s ListenerStats) String() string {
	total := s.MessagesReceived + s.MessagesRejected
	acceptRate := 0.0
	if total > 0 {
		acceptRate = 100.0 * float64(s.MessagesReceived) / float64(total)
	}

	return fmt.Sprintf("Messages: %d received, %d rejected (%.1f%% accept rate)",
		s.MessagesReceived, s.MessagesRejected, acceptRate)
}
