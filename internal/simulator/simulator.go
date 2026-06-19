package simulator

import (
	"context"
	"fmt"
	"sentinelstream/internal/protocol"
	"sync"
	"time"
)

// SimulatorConfig holds configuration for the simulator.
type SimulatorConfig struct {
	// NumberOfSensors is the number of sensors to simulate.
	NumberOfSensors int
	// MessageRateHz is the message rate in Hz (messages per second).
	MessageRateHz int
	// SensorTypes specifies which sensor types to simulate.
	SensorTypes []string
}

// Simulator orchestrates multiple sensor generators.
type Simulator struct {
	config     SimulatorConfig
	generators []SensorGenerator
	mu         sync.Mutex
}

// NewSimulator creates a new simulator with the given configuration.
func NewSimulator(config SimulatorConfig) *Simulator {
	sim := &Simulator{
		config:     config,
		generators: []SensorGenerator{},
	}

	if len(config.SensorTypes) == 0 {
		config.SensorTypes = []string{"gps", "drone", "vehicle", "temperature"}
	}

	sensorIndex := 0

	for _, sensorType := range config.SensorTypes {
		for i := 0; i < config.NumberOfSensors; i++ {
			id := fmt.Sprintf("%s-%d", sensorType, i)
			seed := int64(sensorIndex*1000 + i)

			var gen SensorGenerator
			switch sensorType {
			case "gps":
				gen = NewGPSSensorGenerator(id, seed)
			case "drone":
				gen = NewDroneSensorGenerator(id, seed)
			case "vehicle":
				gen = NewVehicleSensorGenerator(id, seed)
			case "temperature":
				gen = NewTemperatureSensorGenerator(id, seed)
			default:
				continue
			}

			sim.generators = append(sim.generators, gen)
			sensorIndex++
		}
	}

	return sim
}

// Run starts the simulator and sends messages to the given channel.
// It runs until the context is cancelled.
func (s *Simulator) Run(ctx context.Context, messages chan<- *protocol.TelemetryMessage) error {
	if len(s.generators) == 0 {
		return fmt.Errorf("no sensors configured")
	}

	interval := time.Duration(1000/s.config.MessageRateHz) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	generatorIndex := 0
	totalSent := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Simulator stopped. Total messages sent: %d\n", totalSent)
			return ctx.Err()
		case <-ticker.C:
			// Get the next generator in round-robin order
			gen := s.generators[generatorIndex]
			msg := gen.Next()
			totalSent++

			select {
			case messages <- msg:
				// Message sent successfully
			case <-ctx.Done():
				return ctx.Err()
			}

			// Move to next generator
			generatorIndex = (generatorIndex + 1) % len(s.generators)
		}
	}
}

// SensorCount returns the total number of sensors being simulated.
func (s *Simulator) SensorCount() int {
	return len(s.generators)
}
