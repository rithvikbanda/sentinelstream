package protocol

import (
	"encoding/json"
	"time"
)

// TelemetryMessage represents a single telemetry event from a sensor.
type TelemetryMessage struct {
	SensorID   string          `json:"sensor_id"`
	SensorType string          `json:"sensor_type"`
	Sequence   uint64          `json:"sequence"`
	Timestamp  time.Time       `json:"timestamp"`
	Data       json.RawMessage `json:"data"`
}

// GPSData represents GPS tracker telemetry.
type GPSData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
	Speed     float64 `json:"speed"`
	Heading   float64 `json:"heading"`
}

// DroneData represents drone telemetry.
type DroneData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
	Battery   int     `json:"battery"`
	Signal    int     `json:"signal"`
}

// RadarData represents radar system telemetry.
type RadarData struct {
	TargetCount int     `json:"target_count"`
	MaxRange    float64 `json:"max_range"`
	Operational bool    `json:"operational"`
}

// TemperatureSensorData represents temperature sensor telemetry.
type TemperatureSensorData struct {
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	Unit        string  `json:"unit"`
}

// PressureSensorData represents pressure sensor telemetry.
type PressureSensorData struct {
	Pressure float64 `json:"pressure"`
	Unit     string  `json:"unit"`
}

// VibrationSensorData represents vibration sensor telemetry.
type VibrationSensorData struct {
	Frequency float64 `json:"frequency"`
	Amplitude float64 `json:"amplitude"`
	Unit      string  `json:"unit"`
}

// VehicleData represents fleet vehicle telemetry.
type VehicleData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Speed     float64 `json:"speed"`
	Fuel      float64 `json:"fuel"`
	Mileage   float64 `json:"mileage"`
}

// AssetTrackerData represents asset/logistics tracker telemetry.
type AssetTrackerData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Temperature float64 `json:"temperature"`
	Humidity  float64 `json:"humidity"`
}

// PowerGridData represents power grid monitor telemetry.
type PowerGridData struct {
	Voltage   float64 `json:"voltage"`
	Current   float64 `json:"current"`
	Frequency float64 `json:"frequency"`
	PowerFactor float64 `json:"power_factor"`
}

// WeatherStationData represents weather station telemetry.
type WeatherStationData struct {
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	Pressure    float64 `json:"pressure"`
	WindSpeed   float64 `json:"wind_speed"`
	Precipitation float64 `json:"precipitation"`
}

// PatientVitalsData represents patient vital signs telemetry.
type PatientVitalsData struct {
	HeartRate   int     `json:"heart_rate"`
	BloodPressure string `json:"blood_pressure"`
	Temperature float64 `json:"temperature"`
	SpO2        int     `json:"spo2"`
}

// MedicalDeviceData represents medical device telemetry.
type MedicalDeviceData struct {
	DeviceID    string  `json:"device_id"`
	Status      string  `json:"status"`
	Battery     int     `json:"battery"`
	LastCal     string  `json:"last_calibration"`
}
