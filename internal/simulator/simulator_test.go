package simulator

import (
	"encoding/json"
	"testing"
	"time"
)

func TestGPSSensorGenerator_Sequence(t *testing.T) {
	gen := NewGPSSensorGenerator("gps-1", 12345)

	msg1 := gen.Next()
	msg2 := gen.Next()
	msg3 := gen.Next()

	if msg1.Sequence != 1 {
		t.Errorf("expected sequence 1, got %d", msg1.Sequence)
	}
	if msg2.Sequence != 2 {
		t.Errorf("expected sequence 2, got %d", msg2.Sequence)
	}
	if msg3.Sequence != 3 {
		t.Errorf("expected sequence 3, got %d", msg3.Sequence)
	}
}

func TestGPSSensorGenerator_SensorID(t *testing.T) {
	gen := NewGPSSensorGenerator("gps-test-1", 12345)

	if gen.SensorID() != "gps-test-1" {
		t.Errorf("expected sensor ID gps-test-1, got %s", gen.SensorID())
	}
}

func TestGPSSensorGenerator_SensorType(t *testing.T) {
	gen := NewGPSSensorGenerator("gps-1", 12345)

	if gen.SensorType() != "gps" {
		t.Errorf("expected sensor type gps, got %s", gen.SensorType())
	}
}

func TestGPSSensorGenerator_ValidCoordinates(t *testing.T) {
	gen := NewGPSSensorGenerator("gps-1", 12345)

	for i := 0; i < 100; i++ {
		msg := gen.Next()

		// Parse GPS data
		gpsData, err := unmarshalGPSData(msg.Data)
		if err != nil {
			t.Fatalf("failed to unmarshal GPS data: %v", err)
		}

		if gpsData.Latitude < -90 || gpsData.Latitude > 90 {
			t.Errorf("latitude out of bounds: %f", gpsData.Latitude)
		}
		if gpsData.Longitude < -180 || gpsData.Longitude > 180 {
			t.Errorf("longitude out of bounds: %f", gpsData.Longitude)
		}
		if gpsData.Altitude < 0 {
			t.Errorf("altitude negative: %f", gpsData.Altitude)
		}
		if gpsData.Speed < 0 {
			t.Errorf("speed negative: %f", gpsData.Speed)
		}
	}
}

func TestDroneSensorGenerator_BatteryRange(t *testing.T) {
	gen := NewDroneSensorGenerator("drone-1", 12345)

	for i := 0; i < 100; i++ {
		msg := gen.Next()

		droneData, err := unmarshalDroneData(msg.Data)
		if err != nil {
			t.Fatalf("failed to unmarshal drone data: %v", err)
		}

		if droneData.Battery < 0 || droneData.Battery > 100 {
			t.Errorf("battery out of range: %d", droneData.Battery)
		}
	}
}

func TestVehicleSensorGenerator_Sequence(t *testing.T) {
	gen := NewVehicleSensorGenerator("vehicle-1", 12345)

	msg1 := gen.Next()
	msg2 := gen.Next()

	if msg1.Sequence != 1 {
		t.Errorf("expected sequence 1, got %d", msg1.Sequence)
	}
	if msg2.Sequence != 2 {
		t.Errorf("expected sequence 2, got %d", msg2.Sequence)
	}
}

func TestTemperatureSensorGenerator_HumidityRange(t *testing.T) {
	gen := NewTemperatureSensorGenerator("temp-1", 12345)

	for i := 0; i < 100; i++ {
		msg := gen.Next()

		tempData, err := unmarshalTemperatureData(msg.Data)
		if err != nil {
			t.Fatalf("failed to unmarshal temperature data: %v", err)
		}

		if tempData.Humidity < 0 || tempData.Humidity > 100 {
			t.Errorf("humidity out of range: %f", tempData.Humidity)
		}
	}
}

func TestSensorGenerator_Timestamp(t *testing.T) {
	gen := NewGPSSensorGenerator("gps-1", 12345)

	beforeTime := time.Now().UTC()
	msg := gen.Next()
	afterTime := time.Now().UTC()

	if msg.Timestamp.Before(beforeTime) || msg.Timestamp.After(afterTime.Add(1*time.Second)) {
		t.Errorf("timestamp outside expected range: %v", msg.Timestamp)
	}
}

// Helper functions for unmarshaling

func unmarshalGPSData(data []byte) (*GPSData, error) {
	var gpsData GPSData
	err := json.Unmarshal(data, &gpsData)
	return &gpsData, err
}

func unmarshalDroneData(data []byte) (*DroneData, error) {
	var droneData DroneData
	err := json.Unmarshal(data, &droneData)
	return &droneData, err
}

func unmarshalTemperatureData(data []byte) (*TemperatureData, error) {
	var tempData TemperatureData
	err := json.Unmarshal(data, &tempData)
	return &tempData, err
}

type GPSData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
	Speed     float64 `json:"speed"`
	Heading   float64 `json:"heading"`
}

type DroneData struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Altitude  float64 `json:"altitude"`
	Battery   int     `json:"battery"`
	Signal    int     `json:"signal"`
}

type TemperatureData struct {
	Temperature float64 `json:"temperature"`
	Humidity    float64 `json:"humidity"`
	Unit        string  `json:"unit"`
}
