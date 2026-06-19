package protocol

import (
	"fmt"
	"time"
)

// ValidationError represents a validation failure.
type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation error in %s: %s", e.Field, e.Reason)
}

// ValidateTelemetryMessage checks if a TelemetryMessage is well-formed.
func ValidateTelemetryMessage(msg *TelemetryMessage) error {
	if msg.SensorID == "" {
		return ValidationError{Field: "sensor_id", Reason: "required"}
	}
	if msg.SensorType == "" {
		return ValidationError{Field: "sensor_type", Reason: "required"}
	}
	if msg.Timestamp.IsZero() {
		return ValidationError{Field: "timestamp", Reason: "required"}
	}
	if msg.Timestamp.After(time.Now().Add(5 * time.Minute)) {
		return ValidationError{Field: "timestamp", Reason: "cannot be in the future"}
	}
	if len(msg.Data) == 0 {
		return ValidationError{Field: "data", Reason: "required"}
	}
	return nil
}

// ValidateGPSData checks GPS payload constraints.
func ValidateGPSData(data *GPSData) error {
	if data.Latitude < -90 || data.Latitude > 90 {
		return ValidationError{Field: "latitude", Reason: "must be between -90 and 90"}
	}
	if data.Longitude < -180 || data.Longitude > 180 {
		return ValidationError{Field: "longitude", Reason: "must be between -180 and 180"}
	}
	if data.Altitude < 0 {
		return ValidationError{Field: "altitude", Reason: "must be non-negative"}
	}
	if data.Speed < 0 {
		return ValidationError{Field: "speed", Reason: "must be non-negative"}
	}
	return nil
}

// ValidateDroneData checks drone payload constraints.
func ValidateDroneData(data *DroneData) error {
	if data.Latitude < -90 || data.Latitude > 90 {
		return ValidationError{Field: "latitude", Reason: "must be between -90 and 90"}
	}
	if data.Longitude < -180 || data.Longitude > 180 {
		return ValidationError{Field: "longitude", Reason: "must be between -180 and 180"}
	}
	if data.Altitude < 0 {
		return ValidationError{Field: "altitude", Reason: "must be non-negative"}
	}
	if data.Battery < 0 || data.Battery > 100 {
		return ValidationError{Field: "battery", Reason: "must be between 0 and 100"}
	}
	if data.Signal < 0 || data.Signal > 100 {
		return ValidationError{Field: "signal", Reason: "must be between 0 and 100"}
	}
	return nil
}

// ValidateRadarData checks radar payload constraints.
func ValidateRadarData(data *RadarData) error {
	if data.TargetCount < 0 {
		return ValidationError{Field: "target_count", Reason: "must be non-negative"}
	}
	if data.MaxRange <= 0 {
		return ValidationError{Field: "max_range", Reason: "must be positive"}
	}
	return nil
}

// ValidateTemperatureSensorData checks temperature sensor constraints.
func ValidateTemperatureSensorData(data *TemperatureSensorData) error {
	if data.Humidity < 0 || data.Humidity > 100 {
		return ValidationError{Field: "humidity", Reason: "must be between 0 and 100"}
	}
	if data.Unit == "" {
		return ValidationError{Field: "unit", Reason: "required"}
	}
	return nil
}

// ValidateDroneDataPayload checks drone battery is in valid range (0-100).
func ValidateBatteryPercentage(battery int) error {
	if battery < 0 || battery > 100 {
		return ValidationError{Field: "battery", Reason: "must be between 0 and 100"}
	}
	return nil
}

// ValidateVehicleData checks vehicle payload constraints.
func ValidateVehicleData(data *VehicleData) error {
	if data.Latitude < -90 || data.Latitude > 90 {
		return ValidationError{Field: "latitude", Reason: "must be between -90 and 90"}
	}
	if data.Longitude < -180 || data.Longitude > 180 {
		return ValidationError{Field: "longitude", Reason: "must be between -180 and 180"}
	}
	if data.Speed < 0 {
		return ValidationError{Field: "speed", Reason: "must be non-negative"}
	}
	if data.Fuel < 0 {
		return ValidationError{Field: "fuel", Reason: "must be non-negative"}
	}
	if data.Mileage < 0 {
		return ValidationError{Field: "mileage", Reason: "must be non-negative"}
	}
	return nil
}

// ValidatePatientVitals checks patient vitals constraints.
func ValidatePatientVitals(data *PatientVitalsData) error {
	if data.HeartRate < 0 || data.HeartRate > 300 {
		return ValidationError{Field: "heart_rate", Reason: "must be between 0 and 300"}
	}
	if data.Temperature < 35 || data.Temperature > 42 {
		return ValidationError{Field: "temperature", Reason: "must be between 35 and 42 Celsius"}
	}
	if data.SpO2 < 0 || data.SpO2 > 100 {
		return ValidationError{Field: "spo2", Reason: "must be between 0 and 100"}
	}
	return nil
}
