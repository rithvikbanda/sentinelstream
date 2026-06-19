package protocol

import (
	"encoding/json"
	"fmt"
)

// ParseTelemetryMessage parses raw JSON into a TelemetryMessage and validates it.
func ParseTelemetryMessage(rawJSON []byte) (*TelemetryMessage, error) {
	var msg TelemetryMessage
	if err := json.Unmarshal(rawJSON, &msg); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	if err := ValidateTelemetryMessage(&msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// ParseGPSData parses GPS payload from a TelemetryMessage.
func ParseGPSData(msg *TelemetryMessage) (*GPSData, error) {
	var data GPSData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse GPS data: %w", err)
	}
	if err := ValidateGPSData(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParseDroneData parses drone payload from a TelemetryMessage.
func ParseDroneData(msg *TelemetryMessage) (*DroneData, error) {
	var data DroneData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse drone data: %w", err)
	}
	if err := ValidateDroneData(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParseRadarData parses radar payload from a TelemetryMessage.
func ParseRadarData(msg *TelemetryMessage) (*RadarData, error) {
	var data RadarData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse radar data: %w", err)
	}
	if err := ValidateRadarData(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParseTemperatureSensorData parses temperature sensor payload.
func ParseTemperatureSensorData(msg *TelemetryMessage) (*TemperatureSensorData, error) {
	var data TemperatureSensorData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse temperature data: %w", err)
	}
	if err := ValidateTemperatureSensorData(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParsePressureSensorData parses pressure sensor payload.
func ParsePressureSensorData(msg *TelemetryMessage) (*PressureSensorData, error) {
	var data PressureSensorData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse pressure data: %w", err)
	}
	return &data, nil
}

// ParseVibrationSensorData parses vibration sensor payload.
func ParseVibrationSensorData(msg *TelemetryMessage) (*VibrationSensorData, error) {
	var data VibrationSensorData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse vibration data: %w", err)
	}
	return &data, nil
}

// ParseVehicleData parses vehicle payload.
func ParseVehicleData(msg *TelemetryMessage) (*VehicleData, error) {
	var data VehicleData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse vehicle data: %w", err)
	}
	if err := ValidateVehicleData(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParseAssetTrackerData parses asset tracker payload.
func ParseAssetTrackerData(msg *TelemetryMessage) (*AssetTrackerData, error) {
	var data AssetTrackerData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse asset tracker data: %w", err)
	}
	return &data, nil
}

// ParsePowerGridData parses power grid payload.
func ParsePowerGridData(msg *TelemetryMessage) (*PowerGridData, error) {
	var data PowerGridData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse power grid data: %w", err)
	}
	return &data, nil
}

// ParseWeatherStationData parses weather station payload.
func ParseWeatherStationData(msg *TelemetryMessage) (*WeatherStationData, error) {
	var data WeatherStationData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse weather station data: %w", err)
	}
	return &data, nil
}

// ParsePatientVitals parses patient vitals payload.
func ParsePatientVitals(msg *TelemetryMessage) (*PatientVitalsData, error) {
	var data PatientVitalsData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse patient vitals: %w", err)
	}
	if err := ValidatePatientVitals(&data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ParseMedicalDeviceData parses medical device payload.
func ParseMedicalDeviceData(msg *TelemetryMessage) (*MedicalDeviceData, error) {
	var data MedicalDeviceData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return nil, fmt.Errorf("failed to parse medical device data: %w", err)
	}
	return &data, nil
}
