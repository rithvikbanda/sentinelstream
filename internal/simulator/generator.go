package simulator

import (
	"encoding/json"
	"math"
	"math/rand"
	"sentinelstream/internal/protocol"
	"time"
)

// SensorGenerator produces telemetry messages for a specific sensor.
type SensorGenerator interface {
	// Next generates the next telemetry message for this sensor.
	Next() *protocol.TelemetryMessage
	// SensorID returns the unique sensor identifier.
	SensorID() string
	// SensorType returns the sensor type.
	SensorType() string
}

// GPSSensorGenerator generates GPS tracker telemetry.
type GPSSensorGenerator struct {
	id       string
	sequence uint64
	lat      float64
	lon      float64
	alt      float64
	speed    float64
	heading  float64
	rng      *rand.Rand
}

// NewGPSSensorGenerator creates a new GPS sensor generator.
func NewGPSSensorGenerator(id string, seed int64) *GPSSensorGenerator {
	return &GPSSensorGenerator{
		id:       id,
		sequence: 0,
		lat:      47.6 + (rand.Float64()-0.5)*0.2, // around Seattle
		lon:      -122.3 + (rand.Float64()-0.5)*0.2,
		alt:      rand.Float64() * 500,
		speed:    rand.Float64() * 100,
		heading:  rand.Float64() * 360,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (g *GPSSensorGenerator) SensorID() string {
	return g.id
}

func (g *GPSSensorGenerator) SensorType() string {
	return "gps"
}

func (g *GPSSensorGenerator) Next() *protocol.TelemetryMessage {
	g.sequence++

	// Simulate movement: small random walk
	g.lat += (g.rng.Float64() - 0.5) * 0.001
	g.lon += (g.rng.Float64() - 0.5) * 0.001
	g.alt += (g.rng.Float64() - 0.5) * 10
	g.speed += (g.rng.Float64() - 0.5) * 5
	g.heading += (g.rng.Float64() - 0.5) * 10

	g.lat = math.Max(-90, math.Min(90, g.lat))
	g.lon = math.Max(-180, math.Min(180, g.lon))
	g.alt = math.Max(0, g.alt)
	g.speed = math.Max(0, math.Min(150, g.speed))
	g.heading = math.Mod(g.heading, 360)

	data := protocol.GPSData{
		Latitude:  g.lat,
		Longitude: g.lon,
		Altitude:  g.alt,
		Speed:     g.speed,
		Heading:   g.heading,
	}

	dataJSON, _ := json.Marshal(data)

	return &protocol.TelemetryMessage{
		SensorID:   g.id,
		SensorType: g.SensorType(),
		Sequence:   g.sequence,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}
}

// DroneSensorGenerator generates drone telemetry.
type DroneSensorGenerator struct {
	id       string
	sequence uint64
	lat      float64
	lon      float64
	alt      float64
	battery  int
	signal   int
	rng      *rand.Rand
}

// NewDroneSensorGenerator creates a new drone sensor generator.
func NewDroneSensorGenerator(id string, seed int64) *DroneSensorGenerator {
	return &DroneSensorGenerator{
		id:       id,
		sequence: 0,
		lat:      47.6 + (rand.Float64()-0.5)*0.1,
		lon:      -122.3 + (rand.Float64()-0.5)*0.1,
		alt:      rand.Float64() * 300,
		battery:  80 + rand.Intn(20),
		signal:   70 + rand.Intn(30),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (g *DroneSensorGenerator) SensorID() string {
	return g.id
}

func (g *DroneSensorGenerator) SensorType() string {
	return "drone"
}

func (g *DroneSensorGenerator) Next() *protocol.TelemetryMessage {
	g.sequence++

	// Simulate flight path
	g.lat += (g.rng.Float64() - 0.5) * 0.002
	g.lon += (g.rng.Float64() - 0.5) * 0.002
	g.alt += (g.rng.Float64() - 0.5) * 15

	g.lat = math.Max(-90, math.Min(90, g.lat))
	g.lon = math.Max(-180, math.Min(180, g.lon))
	g.alt = math.Max(0, math.Min(500, g.alt))

	// Battery drains slowly
	g.battery = int(math.Max(0, float64(g.battery)-(g.rng.Float64()*2)))
	g.signal = 50 + g.rng.Intn(50)

	data := protocol.DroneData{
		Latitude:  g.lat,
		Longitude: g.lon,
		Altitude:  g.alt,
		Battery:   g.battery,
		Signal:    g.signal,
	}

	dataJSON, _ := json.Marshal(data)

	return &protocol.TelemetryMessage{
		SensorID:   g.id,
		SensorType: g.SensorType(),
		Sequence:   g.sequence,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}
}

// VehicleSensorGenerator generates vehicle fleet telemetry.
type VehicleSensorGenerator struct {
	id       string
	sequence uint64
	lat      float64
	lon      float64
	speed    float64
	fuel     float64
	mileage  float64
	rng      *rand.Rand
}

// NewVehicleSensorGenerator creates a new vehicle sensor generator.
func NewVehicleSensorGenerator(id string, seed int64) *VehicleSensorGenerator {
	return &VehicleSensorGenerator{
		id:       id,
		sequence: 0,
		lat:      47.6 + (rand.Float64()-0.5)*0.3,
		lon:      -122.3 + (rand.Float64()-0.5)*0.3,
		speed:    rand.Float64() * 80,
		fuel:     rand.Float64() * 100,
		mileage:  rand.Float64() * 100000,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (g *VehicleSensorGenerator) SensorID() string {
	return g.id
}

func (g *VehicleSensorGenerator) SensorType() string {
	return "vehicle"
}

func (g *VehicleSensorGenerator) Next() *protocol.TelemetryMessage {
	g.sequence++

	// Simulate driving
	g.lat += (g.rng.Float64() - 0.5) * 0.001
	g.lon += (g.rng.Float64() - 0.5) * 0.001
	g.speed = math.Max(0, math.Min(120, g.speed+(g.rng.Float64()-0.5)*20))
	g.fuel = math.Max(0, g.fuel-(g.rng.Float64()*0.1))
	g.mileage += g.speed / 10

	g.lat = math.Max(-90, math.Min(90, g.lat))
	g.lon = math.Max(-180, math.Min(180, g.lon))

	data := protocol.VehicleData{
		Latitude: g.lat,
		Longitude: g.lon,
		Speed:    g.speed,
		Fuel:     g.fuel,
		Mileage:  g.mileage,
	}

	dataJSON, _ := json.Marshal(data)

	return &protocol.TelemetryMessage{
		SensorID:   g.id,
		SensorType: g.SensorType(),
		Sequence:   g.sequence,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}
}

// TemperatureSensorGenerator generates temperature/humidity telemetry.
type TemperatureSensorGenerator struct {
	id       string
	sequence uint64
	temp     float64
	humidity float64
	rng      *rand.Rand
}

// NewTemperatureSensorGenerator creates a new temperature sensor generator.
func NewTemperatureSensorGenerator(id string, seed int64) *TemperatureSensorGenerator {
	return &TemperatureSensorGenerator{
		id:       id,
		sequence: 0,
		temp:     20 + rand.Float64()*10,
		humidity: 30 + rand.Float64()*40,
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (g *TemperatureSensorGenerator) SensorID() string {
	return g.id
}

func (g *TemperatureSensorGenerator) SensorType() string {
	return "temperature"
}

func (g *TemperatureSensorGenerator) Next() *protocol.TelemetryMessage {
	g.sequence++

	// Simulate gradual temperature changes
	g.temp += (g.rng.Float64() - 0.5) * 0.5
	g.humidity += (g.rng.Float64() - 0.5) * 2

	g.temp = math.Max(0, math.Min(50, g.temp))
	g.humidity = math.Max(0, math.Min(100, g.humidity))

	data := protocol.TemperatureSensorData{
		Temperature: g.temp,
		Humidity:    g.humidity,
		Unit:        "celsius",
	}

	dataJSON, _ := json.Marshal(data)

	return &protocol.TelemetryMessage{
		SensorID:   g.id,
		SensorType: g.SensorType(),
		Sequence:   g.sequence,
		Timestamp:  time.Now().UTC(),
		Data:       dataJSON,
	}
}
