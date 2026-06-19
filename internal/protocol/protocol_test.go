package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseTelemetryMessage_Valid(t *testing.T) {
	rawJSON := []byte(`{
		"sensor_id": "drone-17",
		"sensor_type": "drone",
		"sequence": 1042,
		"timestamp": "2026-06-18T16:30:21.314Z",
		"data": {"latitude": 47.674, "longitude": -122.121, "altitude": 152.4, "battery": 72}
	}`)

	msg, err := ParseTelemetryMessage(rawJSON)
	if err != nil {
		t.Fatalf("ParseTelemetryMessage failed: %v", err)
	}
	if msg.SensorID != "drone-17" {
		t.Errorf("expected sensor_id drone-17, got %s", msg.SensorID)
	}
	if msg.Sequence != 1042 {
		t.Errorf("expected sequence 1042, got %d", msg.Sequence)
	}
}

func TestParseTelemetryMessage_MissingField(t *testing.T) {
	rawJSON := []byte(`{
		"sensor_type": "drone",
		"sequence": 1042,
		"timestamp": "2026-06-18T16:30:21.314Z",
		"data": {}
	}`)

	_, err := ParseTelemetryMessage(rawJSON)
	if err == nil {
		t.Fatal("expected validation error for missing sensor_id")
	}
}

func TestParseTelemetryMessage_InvalidJSON(t *testing.T) {
	rawJSON := []byte(`{invalid json}`)

	_, err := ParseTelemetryMessage(rawJSON)
	if err == nil {
		t.Fatal("expected parsing error for invalid JSON")
	}
}

func TestParseDroneData_Valid(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "drone-17",
		SensorType: "drone",
		Sequence:   1042,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 47.674, "longitude": -122.121, "altitude": 152.4, "battery": 72, "signal": 95}`),
	}

	data, err := ParseDroneData(msg)
	if err != nil {
		t.Fatalf("ParseDroneData failed: %v", err)
	}
	if data.Battery != 72 {
		t.Errorf("expected battery 72, got %d", data.Battery)
	}
}

func TestParseDroneData_InvalidBattery(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "drone-17",
		SensorType: "drone",
		Sequence:   1042,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 47.674, "longitude": -122.121, "altitude": 152.4, "battery": 145, "signal": 95}`),
	}

	_, err := ParseDroneData(msg)
	if err == nil {
		t.Fatal("expected validation error for battery > 100")
	}
}

func TestParseGPSData_ValidCoordinates(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "gps-1",
		SensorType: "gps",
		Sequence:   100,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 47.674, "longitude": -122.121, "altitude": 152.4, "speed": 25.5, "heading": 180}`),
	}

	data, err := ParseGPSData(msg)
	if err != nil {
		t.Fatalf("ParseGPSData failed: %v", err)
	}
	if data.Latitude != 47.674 {
		t.Errorf("expected latitude 47.674, got %f", data.Latitude)
	}
}

func TestParseGPSData_InvalidLatitude(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "gps-1",
		SensorType: "gps",
		Sequence:   100,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 95, "longitude": -122.121, "altitude": 152.4, "speed": 25.5, "heading": 180}`),
	}

	_, err := ParseGPSData(msg)
	if err == nil {
		t.Fatal("expected validation error for latitude > 90")
	}
}

func TestParseGPSData_InvalidLongitude(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "gps-1",
		SensorType: "gps",
		Sequence:   100,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 47.674, "longitude": 190, "altitude": 152.4, "speed": 25.5, "heading": 180}`),
	}

	_, err := ParseGPSData(msg)
	if err == nil {
		t.Fatal("expected validation error for longitude > 180")
	}
}

func TestValidateBatteryPercentage(t *testing.T) {
	tests := []struct {
		name      string
		battery   int
		expectErr bool
	}{
		{"valid 0", 0, false},
		{"valid 50", 50, false},
		{"valid 100", 100, false},
		{"invalid -1", -1, true},
		{"invalid 101", 101, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBatteryPercentage(tt.battery)
			if (err != nil) != tt.expectErr {
				t.Errorf("ValidateBatteryPercentage(%d): got err=%v, expectErr=%v", tt.battery, err, tt.expectErr)
			}
		})
	}
}

func TestParsePatientVitals_Valid(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "patient-001",
		SensorType: "patient_vitals",
		Sequence:   50,
		Timestamp:  time.Now(),
		Data:       []byte(`{"heart_rate": 72, "blood_pressure": "120/80", "temperature": 37.2, "spo2": 98}`),
	}

	data, err := ParsePatientVitals(msg)
	if err != nil {
		t.Fatalf("ParsePatientVitals failed: %v", err)
	}
	if data.HeartRate != 72 {
		t.Errorf("expected heart_rate 72, got %d", data.HeartRate)
	}
}

func TestParsePatientVitals_InvalidHeartRate(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "patient-001",
		SensorType: "patient_vitals",
		Sequence:   50,
		Timestamp:  time.Now(),
		Data:       []byte(`{"heart_rate": 400, "blood_pressure": "120/80", "temperature": 37.2, "spo2": 98}`),
	}

	_, err := ParsePatientVitals(msg)
	if err == nil {
		t.Fatal("expected validation error for heart_rate > 300")
	}
}

func TestParsePatientVitals_InvalidTemperature(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "patient-001",
		SensorType: "patient_vitals",
		Sequence:   50,
		Timestamp:  time.Now(),
		Data:       []byte(`{"heart_rate": 72, "blood_pressure": "120/80", "temperature": 50, "spo2": 98}`),
	}

	_, err := ParsePatientVitals(msg)
	if err == nil {
		t.Fatal("expected validation error for temperature > 42")
	}
}

func TestParseVehicleData_Valid(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "vehicle-001",
		SensorType: "vehicle",
		Sequence:   200,
		Timestamp:  time.Now(),
		Data:       []byte(`{"latitude": 47.674, "longitude": -122.121, "speed": 65.5, "fuel": 75.0, "mileage": 15000.5}`),
	}

	data, err := ParseVehicleData(msg)
	if err != nil {
		t.Fatalf("ParseVehicleData failed: %v", err)
	}
	if data.Speed != 65.5 {
		t.Errorf("expected speed 65.5, got %f", data.Speed)
	}
}

func TestTelemetryMessage_JSONMarshal(t *testing.T) {
	msg := &TelemetryMessage{
		SensorID:   "test-sensor",
		SensorType: "test",
		Sequence:   1,
		Timestamp:  time.Date(2026, 6, 18, 16, 30, 21, 314000000, time.UTC),
		Data:       json.RawMessage(`{"test": "data"}`),
	}

	jsonBytes, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	var decoded TelemetryMessage
	if err := json.Unmarshal(jsonBytes, &decoded); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	if decoded.SensorID != msg.SensorID {
		t.Errorf("roundtrip failed: sensor_id mismatch")
	}
}
