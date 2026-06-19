// Package metrics provides lightweight, dependency-free latency tracking
// for reporting p50/p95/p99 percentiles.
package metrics

import (
	"sort"
	"sync"
	"time"
)

// defaultCapacity bounds memory use: once full, a LatencyRecorder overwrites
// the oldest sample rather than growing without limit, trading a bit of
// statistical staleness under sustained load for O(1) memory.
const defaultCapacity = 10000

// LatencyRecorder collects latency samples in a fixed-size circular buffer
// and reports percentiles over whatever samples are currently held.
type LatencyRecorder struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	full    bool
}

// NewLatencyRecorder creates a recorder that holds up to capacity samples.
// A capacity <= 0 uses a sensible default.
func NewLatencyRecorder(capacity int) *LatencyRecorder {
	if capacity <= 0 {
		capacity = defaultCapacity
	}
	return &LatencyRecorder{samples: make([]time.Duration, capacity)}
}

// Record adds a latency sample.
func (r *LatencyRecorder) Record(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.samples[r.next] = d
	r.next++
	if r.next == len(r.samples) {
		r.next = 0
		r.full = true
	}
}

// Snapshot is a point-in-time percentile summary.
type Snapshot struct {
	Count int
	Min   time.Duration
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

// Snapshot computes percentiles over the samples currently held. It is safe
// to call concurrently with Record.
func (r *LatencyRecorder) Snapshot() Snapshot {
	r.mu.Lock()
	n := len(r.samples)
	if !r.full {
		n = r.next
	}
	data := make([]time.Duration, n)
	copy(data, r.samples[:n])
	r.mu.Unlock()

	if n == 0 {
		return Snapshot{}
	}

	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	percentile := func(p float64) time.Duration {
		idx := int(p * float64(n-1))
		return data[idx]
	}

	return Snapshot{
		Count: n,
		Min:   data[0],
		P50:   percentile(0.50),
		P95:   percentile(0.95),
		P99:   percentile(0.99),
		Max:   data[n-1],
	}
}
