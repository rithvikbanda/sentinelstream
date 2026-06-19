package ingestion

import (
	"bufio"
	"fmt"
	"log/slog"
	"net"
	"sentinelstream/internal/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// maxTCPLineSize bounds how large a single newline-delimited message may be,
// so a misbehaving or malicious client can't grow a connection's read
// buffer without limit.
const maxTCPLineSize = 1 << 20 // 1 MiB

// TCPListener listens for newline-delimited telemetry messages on a TCP port.
// Each connection is handled by its own goroutine so a slow or stalled
// client cannot block reads from any other connection.
type TCPListener struct {
	port         int
	listener     net.Listener
	messagesChan chan *protocol.TelemetryMessage
	errorsChan   chan error
	stopChan     chan struct{}
	wg           sync.WaitGroup
	isRunning    atomic.Bool

	messagesReceived  atomic.Uint64
	messagesRejected  atomic.Uint64
	activeConnections atomic.Int64
	totalConnections  atomic.Uint64

	mu               sync.Mutex
	connections      map[net.Conn]struct{}
	lastErrorTime    time.Time
	lastErrorMessage string
}

// NewTCPListener creates a new TCP listener on the specified port.
func NewTCPListener(port int, bufferSize int) *TCPListener {
	return &TCPListener{
		port:         port,
		messagesChan: make(chan *protocol.TelemetryMessage, bufferSize),
		errorsChan:   make(chan error, 10),
		stopChan:     make(chan struct{}),
		connections:  make(map[net.Conn]struct{}),
	}
}

// Start begins listening for TCP connections on the configured port.
func (l *TCPListener) Start() error {
	if l.isRunning.Load() {
		return fmt.Errorf("listener already running")
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", l.port))
	if err != nil {
		return fmt.Errorf("failed to listen on TCP %d: %w", l.port, err)
	}

	l.listener = ln
	l.isRunning.Store(true)

	l.wg.Add(1)
	go l.acceptLoop()

	return nil
}

// Stop gracefully stops the listener: it stops accepting new connections,
// closes every open connection so blocked reads return, and waits for all
// connection-handling goroutines to exit before closing the output channels.
func (l *TCPListener) Stop() error {
	if !l.isRunning.Load() {
		return nil
	}

	l.isRunning.Store(false)
	close(l.stopChan)

	if l.listener != nil {
		l.listener.Close()
	}

	l.mu.Lock()
	for conn := range l.connections {
		conn.Close()
	}
	l.mu.Unlock()

	l.wg.Wait()
	close(l.messagesChan)
	close(l.errorsChan)

	return nil
}

// Messages returns the channel for receiving valid telemetry messages.
func (l *TCPListener) Messages() <-chan *protocol.TelemetryMessage {
	return l.messagesChan
}

// Errors returns the channel for receiving parsing/validation errors.
func (l *TCPListener) Errors() <-chan error {
	return l.errorsChan
}

// acceptLoop accepts incoming connections and spawns a handler goroutine
// for each one.
func (l *TCPListener) acceptLoop() {
	defer l.wg.Done()

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if !l.isRunning.Load() {
				return
			}

			l.mu.Lock()
			l.lastErrorTime = time.Now()
			l.lastErrorMessage = fmt.Sprintf("accept error: %v", err)
			l.mu.Unlock()

			select {
			case l.errorsChan <- err:
			case <-l.stopChan:
				return
			}
			continue
		}

		l.mu.Lock()
		l.connections[conn] = struct{}{}
		l.mu.Unlock()
		l.activeConnections.Add(1)
		l.totalConnections.Add(1)

		slog.Info("connection_opened", "remote_addr", conn.RemoteAddr().String())

		l.wg.Add(1)
		go l.handleConnection(conn)
	}
}

// handleConnection reads newline-delimited JSON messages from a single
// connection until the client disconnects or the listener is stopped.
func (l *TCPListener) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr()

	defer l.wg.Done()
	defer func() {
		conn.Close()
		l.mu.Lock()
		delete(l.connections, conn)
		l.mu.Unlock()
		l.activeConnections.Add(-1)
		slog.Info("connection_closed", "remote_addr", remoteAddr.String())
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxTCPLineSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // skip blank lines between messages
		}

		msg, err := protocol.ParseTelemetryMessage(line)
		if err != nil {
			l.messagesRejected.Add(1)

			l.mu.Lock()
			l.lastErrorTime = time.Now()
			l.lastErrorMessage = fmt.Sprintf("parse error from %s: %v", remoteAddr, err)
			l.mu.Unlock()

			slog.Warn("validation_failed", "protocol", "tcp", "remote_addr", remoteAddr.String(), "error", err.Error())

			select {
			case l.errorsChan <- fmt.Errorf("from %s: %w", remoteAddr, err):
			case <-l.stopChan:
				return
			}
			continue
		}

		l.messagesReceived.Add(1)

		select {
		case l.messagesChan <- msg:
		case <-l.stopChan:
			return
		}
	}

	if err := scanner.Err(); err != nil {
		l.mu.Lock()
		l.lastErrorTime = time.Now()
		l.lastErrorMessage = fmt.Sprintf("read error from %s: %v", remoteAddr, err)
		l.mu.Unlock()
	}
	// scanner.Scan() returning false with a nil Err() means the client
	// disconnected cleanly (EOF); nothing further to record.
}

// Stats returns current listener statistics.
func (l *TCPListener) Stats() TCPListenerStats {
	l.mu.Lock()
	defer l.mu.Unlock()

	return TCPListenerStats{
		MessagesReceived:  l.messagesReceived.Load(),
		MessagesRejected:  l.messagesRejected.Load(),
		ActiveConnections: l.activeConnections.Load(),
		TotalConnections:  l.totalConnections.Load(),
		LastErrorTime:     l.lastErrorTime,
		LastErrorMessage:  l.lastErrorMessage,
		IsRunning:         l.isRunning.Load(),
	}
}

// TCPListenerStats holds statistics about the TCP listener.
type TCPListenerStats struct {
	MessagesReceived  uint64
	MessagesRejected  uint64
	ActiveConnections int64
	TotalConnections  uint64
	LastErrorTime     time.Time
	LastErrorMessage  string
	IsRunning         bool
}

// String returns a formatted string representation of stats.
func (s TCPListenerStats) String() string {
	total := s.MessagesReceived + s.MessagesRejected
	acceptRate := 0.0
	if total > 0 {
		acceptRate = 100.0 * float64(s.MessagesReceived) / float64(total)
	}

	return fmt.Sprintf("Messages: %d received, %d rejected (%.1f%% accept rate), connections: %d active / %d total",
		s.MessagesReceived, s.MessagesRejected, acceptRate, s.ActiveConnections, s.TotalConnections)
}
