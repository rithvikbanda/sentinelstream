package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestLatencyRecorder_EmptySnapshot(t *testing.T) {
	r := NewLatencyRecorder(10)
	snap := r.Snapshot()
	if snap.Count != 0 {
		t.Errorf("expected count 0, got %d", snap.Count)
	}
}

func TestLatencyRecorder_Percentiles(t *testing.T) {
	r := NewLatencyRecorder(100)

	// Record 1ms, 2ms, ..., 100ms - a uniform distribution makes the
	// expected percentile values easy to reason about.
	for i := 1; i <= 100; i++ {
		r.Record(time.Duration(i) * time.Millisecond)
	}

	snap := r.Snapshot()
	if snap.Count != 100 {
		t.Fatalf("expected count 100, got %d", snap.Count)
	}
	if snap.Min != 1*time.Millisecond {
		t.Errorf("expected min 1ms, got %s", snap.Min)
	}
	if snap.Max != 100*time.Millisecond {
		t.Errorf("expected max 100ms, got %s", snap.Max)
	}
	if snap.P50 != 50*time.Millisecond {
		t.Errorf("expected p50 50ms, got %s", snap.P50)
	}
	if snap.P95 != 95*time.Millisecond {
		t.Errorf("expected p95 95ms, got %s", snap.P95)
	}
	if snap.P99 != 99*time.Millisecond {
		t.Errorf("expected p99 99ms, got %s", snap.P99)
	}
}

func TestLatencyRecorder_OverwritesOldestWhenFull(t *testing.T) {
	r := NewLatencyRecorder(3)

	r.Record(1 * time.Millisecond)
	r.Record(2 * time.Millisecond)
	r.Record(3 * time.Millisecond)
	// Buffer is now full; this overwrites the 1ms sample.
	r.Record(4 * time.Millisecond)

	snap := r.Snapshot()
	if snap.Count != 3 {
		t.Fatalf("expected count to stay at capacity 3, got %d", snap.Count)
	}
	if snap.Min != 2*time.Millisecond {
		t.Errorf("expected min 2ms (1ms should have been evicted), got %s", snap.Min)
	}
	if snap.Max != 4*time.Millisecond {
		t.Errorf("expected max 4ms, got %s", snap.Max)
	}
}

func TestLatencyRecorder_ConcurrentRecordAndSnapshot(t *testing.T) {
	r := NewLatencyRecorder(1000)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Record(time.Duration(n*100+j) * time.Microsecond)
			}
		}(i)
	}

	// Concurrent readers shouldn't race with writers.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				r.Snapshot()
			}
		}
	}()

	wg.Wait()
	close(done)

	snap := r.Snapshot()
	if snap.Count != 1000 {
		t.Errorf("expected count 1000, got %d", snap.Count)
	}
}
