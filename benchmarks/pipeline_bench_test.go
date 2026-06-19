// Package benchmarks measures SentinelStream's processing pipeline under
// various worker counts, sensor cardinalities, and burst-traffic
// conditions. See README.md in this directory for methodology and the
// commands used to produce recorded results.
package benchmarks

import (
	"context"
	"encoding/json"
	"fmt"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/protocol"
	"sentinelstream/internal/state"
	"testing"
	"time"
)

func gpsMessage(sensorID string, seq uint64) *protocol.TelemetryMessage {
	data, _ := json.Marshal(protocol.GPSData{
		Latitude:  47.674,
		Longitude: -122.121,
		Altitude:  152.4,
		Speed:     25.5,
		Heading:   180,
	})
	return &protocol.TelemetryMessage{
		SensorID:   sensorID,
		SensorType: "gps",
		Sequence:   seq,
		Timestamp:  time.Now().UTC(),
		Data:       data,
	}
}

// BenchmarkProcessor_WorkerCounts measures sustained enqueue+process
// throughput across different worker-pool sizes, holding queue capacity
// and traffic pattern (a single sensor sending in order) fixed. The
// reported ns/op is the average time to enqueue and fully drain one
// message; lower is better, and it should fall as workers increase until
// the workload becomes contention- or scheduler-bound rather than
// worker-bound.
func BenchmarkProcessor_WorkerCounts(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			store := state.NewStateStore()
			proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: workers, QueueSize: 10000}, store)
			if err := proc.Start(context.Background()); err != nil {
				b.Fatalf("failed to start processor: %v", err)
			}
			defer proc.Stop()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := proc.Enqueue(gpsMessage("bench-sensor", uint64(i))); err != nil {
					b.Fatalf("enqueue failed: %v", err)
				}
			}
			if err := proc.WaitForQuiescence(30 * time.Second); err != nil {
				b.Fatalf("processor never drained: %v", err)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkProcessor_SensorScaling measures throughput as the number of
// distinct concurrently-tracked sensors increases, holding worker count and
// queue size fixed. More sensors means more entries (and more lock
// contention) in the shared state store, so this isolates that cost from
// pure worker-count scaling.
func BenchmarkProcessor_SensorScaling(b *testing.B) {
	for _, numSensors := range []int{10, 100, 500, 1000} {
		b.Run(fmt.Sprintf("sensors=%d", numSensors), func(b *testing.B) {
			store := state.NewStateStore()
			proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: 8, QueueSize: 10000}, store)
			if err := proc.Start(context.Background()); err != nil {
				b.Fatalf("failed to start processor: %v", err)
			}
			defer proc.Stop()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sensorID := fmt.Sprintf("sensor-%d", i%numSensors)
				if err := proc.Enqueue(gpsMessage(sensorID, uint64(i/numSensors)+1)); err != nil {
					b.Fatalf("enqueue failed: %v", err)
				}
			}
			if err := proc.WaitForQuiescence(30 * time.Second); err != nil {
				b.Fatalf("processor never drained: %v", err)
			}
			b.StopTimer()
		})
	}
}

// BenchmarkProcessor_Latency reports p50/p95/p99 end-to-end pipeline
// latency (from a message's timestamp to fully processed) across worker
// counts, run as custom metrics alongside the standard ns/op.
func BenchmarkProcessor_Latency(b *testing.B) {
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			store := state.NewStateStore()
			proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: workers, QueueSize: 10000}, store)
			if err := proc.Start(context.Background()); err != nil {
				b.Fatalf("failed to start processor: %v", err)
			}
			defer proc.Stop()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := proc.Enqueue(gpsMessage("bench-sensor", uint64(i))); err != nil {
					b.Fatalf("enqueue failed: %v", err)
				}
			}
			if err := proc.WaitForQuiescence(30 * time.Second); err != nil {
				b.Fatalf("processor never drained: %v", err)
			}
			b.StopTimer()

			stats := proc.Stats()
			b.ReportMetric(float64(stats.EndToEndLatency.P50.Microseconds()), "p50_us")
			b.ReportMetric(float64(stats.EndToEndLatency.P95.Microseconds()), "p95_us")
			b.ReportMetric(float64(stats.EndToEndLatency.P99.Microseconds()), "p99_us")
		})
	}
}

// BenchmarkProcessor_Backpressure measures what happens when a burst of
// traffic arrives faster than a single worker can drain it: how many
// TryEnqueue calls succeed vs. get rejected once the queue fills, and how
// many queue-full events the processor records. NumWorkers is fixed at 1 so
// the queue reliably saturates regardless of machine speed, isolating the
// effect of queue capacity on backpressure behavior.
func BenchmarkProcessor_Backpressure(b *testing.B) {
	for _, queueSize := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("queue=%d", queueSize), func(b *testing.B) {
			store := state.NewStateStore()
			proc := processor.NewProcessor(processor.ProcessorConfig{NumWorkers: 1, QueueSize: queueSize}, store)
			if err := proc.Start(context.Background()); err != nil {
				b.Fatalf("failed to start processor: %v", err)
			}
			defer proc.Stop()

			accepted, rejected := 0, 0

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := proc.TryEnqueue(gpsMessage("burst-sensor", uint64(i))); err != nil {
					rejected++
				} else {
					accepted++
				}
			}
			b.StopTimer()

			if err := proc.WaitForQuiescence(30 * time.Second); err != nil {
				b.Fatalf("processor never drained: %v", err)
			}

			b.ReportMetric(float64(accepted), "accepted")
			b.ReportMetric(float64(rejected), "rejected")
			b.ReportMetric(float64(proc.Stats().QueueFullEvents), "queue_full_events")
		})
	}
}
