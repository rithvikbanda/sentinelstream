package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/simulator"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Define CLI flags
	numSensors := flag.Int("sensors", 5, "number of sensors to simulate per type")
	rateHz := flag.Int("rate", 10, "message rate in Hz (messages per second)")
	types := flag.String("types", "gps,drone,vehicle,temperature", "comma-separated sensor types to simulate")
	verbose := flag.Bool("verbose", true, "print all messages (set to false to reduce output)")
	protocolName := flag.String("protocol", "udp", "protocol to send over when --target is set: udp or tcp")
	target := flag.String("target", "", "if set, send messages to this host:port over --protocol instead of just printing locally")

	flag.Parse()

	// Parse sensor types
	sensorTypes := strings.Split(*types, ",")
	for i := range sensorTypes {
		sensorTypes[i] = strings.TrimSpace(sensorTypes[i])
	}

	// Create simulator config
	config := simulator.SimulatorConfig{
		NumberOfSensors: *numSensors,
		MessageRateHz:   *rateHz,
		SensorTypes:     sensorTypes,
	}

	// Create simulator
	sim := simulator.NewSimulator(config)

	// Optionally send messages to a real telemetry server
	var sender simulator.Sender
	if *target != "" {
		var err error
		sender, err = simulator.NewSender(*protocolName, *target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create sender: %v\n", err)
			os.Exit(1)
		}
		defer sender.Close()
	}

	fmt.Printf("=== SentinelStream Sensor Simulator ===\n")
	fmt.Printf("Simulating %d total sensor(s) across %v types\n", sim.SensorCount(), config.SensorTypes)
	fmt.Printf("Message rate: %d Hz\n", config.MessageRateHz)
	if sender != nil {
		fmt.Printf("Sending to %s over %s\n", *target, *protocolName)
	}
	fmt.Printf("Starting simulator... (press Ctrl+C to stop)\n\n")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Message channel
	messages := make(chan *protocol.TelemetryMessage, 100)

	// Run simulator in a goroutine
	go func() {
		err := sim.Run(ctx, messages)
		if err != nil && err != context.Canceled {
			fmt.Fprintf(os.Stderr, "Simulator error: %v\n", err)
		}
		close(messages)
	}()

	// Main loop: print messages and handle shutdown
	messageCount := 0
	startTime := time.Now()
	lastPrintTime := startTime

	for {
		select {
		case <-sigChan:
			fmt.Println("\n\nShutting down...")
			cancel()
			elapsed := time.Since(startTime)
			fmt.Printf("Received %d messages in %v\n", messageCount, elapsed)
			fmt.Printf("Average rate: %.2f msg/sec\n", float64(messageCount)/elapsed.Seconds())
			return

		case msg, ok := <-messages:
			if !ok {
				return // Simulator closed the channel
			}

			messageCount++

			if sender != nil {
				if err := sender.Send(msg); err != nil {
					fmt.Fprintf(os.Stderr, "[SEND_ERROR] %v\n", err)
				}
			}

			// Print detailed output
			if *verbose {
				jsonBytes, _ := json.MarshalIndent(msg, "", "  ")
				fmt.Printf("[%d] %s\n", messageCount, string(jsonBytes))
			}

			// Print summary every 5 seconds
			if time.Since(lastPrintTime) > 5*time.Second {
				elapsed := time.Since(startTime)
				rate := float64(messageCount) / elapsed.Seconds()
				fmt.Printf("... %d messages sent (%.2f msg/sec)\n", messageCount, rate)
				lastPrintTime = time.Now()
			}
		}
	}
}
