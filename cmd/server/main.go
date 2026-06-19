package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sentinelstream/internal/api"
	"sentinelstream/internal/health"
	"sentinelstream/internal/ingestion"
	"sentinelstream/internal/logging"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/replay"
	"sentinelstream/internal/state"
	"syscall"
	"time"
)

func main() {
	// Define CLI flags
	udpPort := flag.Int("port", 9000, "UDP port to listen on")
	tcpPort := flag.Int("tcp-port", 9001, "TCP port to listen on")
	apiAddr := flag.String("api-addr", ":8080", "address for the REST API to listen on")
	bufferSize := flag.Int("buffer", 1000, "size of the message buffer")
	staleTimeout := flag.Duration("stale-timeout", 5*time.Second, "time without a message before a sensor is marked stale")
	healthCheckInterval := flag.Duration("health-check-interval", 1*time.Second, "how often to scan for stale sensors")
	recordFile := flag.String("record", "", "if set, append every accepted message to this JSON Lines file for later replay")

	flag.Parse()

	logging.Init(os.Stdout)

	fmt.Printf("=== SentinelStream Telemetry Server ===\n")
	fmt.Printf("Listening on UDP port %d and TCP port %d\n", *udpPort, *tcpPort)
	fmt.Printf("REST API on %s\n", *apiAddr)
	fmt.Printf("Buffer size: %d messages\n\n", *bufferSize)

	// Create the shared sensor state store and the processor's worker pool
	store := state.NewStateStore()
	procConfig := processor.ProcessorConfig{
		NumWorkers: 8,
		QueueSize:  *bufferSize,
	}
	proc := processor.NewProcessor(procConfig, store)

	if err := proc.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start processor: %v\n", err)
		os.Exit(1)
	}
	defer proc.Stop()

	// Start the background scan that marks inactive sensors as stale
	monitor := health.NewMonitor(health.MonitorConfig{
		StaleTimeout:  *staleTimeout,
		CheckInterval: *healthCheckInterval,
	}, store)
	monitor.Start(context.Background())
	defer monitor.Stop()

	// Create the UDP and TCP listeners; both feed the same processing pipeline
	udpListener := ingestion.NewUDPListener(*udpPort, *bufferSize)
	if err := udpListener.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start UDP listener: %v\n", err)
		os.Exit(1)
	}
	defer udpListener.Stop()

	tcpListener := ingestion.NewTCPListener(*tcpPort, *bufferSize)
	if err := tcpListener.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start TCP listener: %v\n", err)
		os.Exit(1)
	}
	defer tcpListener.Stop()

	// Start the REST API
	apiServer := api.NewServer(*apiAddr, store, proc, udpListener, tcpListener)
	if err := apiServer.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start API server: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		apiServer.Stop(ctx)
	}()

	// Optionally record every accepted message for later replay
	var recorder *replay.EventRecorder
	if *recordFile != "" {
		var err error
		recorder, err = replay.NewEventRecorder(*recordFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open record file: %v\n", err)
			os.Exit(1)
		}
		defer recorder.Close()
		fmt.Printf("Recording accepted events to %s\n", *recordFile)
	}

	fmt.Println("Server started. Press Ctrl+C to stop.")

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	forward := func(msg *protocol.TelemetryMessage) {
		if recorder != nil {
			if err := recorder.Record(msg); err != nil {
				slog.Error("record_failed", "sensor_id", msg.SensorID, "error", err.Error())
			}
		}
		if err := proc.Enqueue(msg); err != nil {
			fmt.Printf("[ENQUEUE_ERROR] %v\n", err)
		}
	}

	// Forward messages from both listeners into the shared processor
	go func() {
		for msg := range udpListener.Messages() {
			forward(msg)
		}
	}()
	go func() {
		for msg := range tcpListener.Messages() {
			forward(msg)
		}
	}()

	// Stats printer
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			udpStats := udpListener.Stats()
			tcpStats := tcpListener.Stats()
			procStats := proc.Stats()

			fmt.Printf("\n=== Server Stats (at %s) ===\n", time.Now().Format("15:04:05"))
			fmt.Printf("UDP: %s\n", udpStats.String())
			fmt.Printf("TCP: %s\n", tcpStats.String())
			fmt.Printf("Processing: %s\n", procStats.String())
		}
	}()

	// Wait for shutdown signal
	<-sigChan

	fmt.Println("\n\nShutting down...")
	slog.Info("server_shutdown")

	// Give goroutines time to finish
	time.Sleep(500 * time.Millisecond)

	// Print final stats
	udpStats := udpListener.Stats()
	tcpStats := tcpListener.Stats()
	procStats := proc.Stats()

	fmt.Printf("\n=== Final Statistics ===\n")
	fmt.Printf("UDP: %s\n", udpStats.String())
	fmt.Printf("TCP: %s\n", tcpStats.String())
	fmt.Printf("Processor: %s\n", procStats.String())

	fmt.Printf("Unique sensors: %d\n", store.Count())

	fmt.Println("\nServer stopped.")
}
