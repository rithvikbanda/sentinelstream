# Benchmarks

This directory contains Go benchmarks for SentinelStream's processing
pipeline (`internal/processor`, `internal/state`). They measure the things
called out in the project's Phase 10 plan: throughput at different worker
counts, throughput/contention as sensor cardinality grows, end-to-end
latency percentiles, and backpressure behavior under burst traffic.

All numbers below come from actually running these benchmarks - per the
project's own rule, no performance numbers are reported here that didn't
come from a real, repeatable run.

## How to run

```bash
# Run everything, with allocation stats:
go test ./benchmarks/... -run=^$ -bench=. -benchmem

# Run one benchmark family:
go test ./benchmarks/... -run=^$ -bench=BenchmarkProcessor_WorkerCounts -benchmem

# Longer, more stable run:
go test ./benchmarks/... -run=^$ -bench=. -benchtime=2s -benchmem

# CPU profile:
go test ./benchmarks/... -run=^$ -bench=. -cpuprofile=cpu.prof
go tool pprof cpu.prof

# Memory profile:
go test ./benchmarks/... -run=^$ -bench=. -memprofile=mem.prof
go tool pprof mem.prof
```

`-run=^$` skips the (non-existent) regular tests in this package so only
benchmarks execute. The structured event log is silenced for the duration
of the run (see `main_test.go`) - otherwise the out-of-order/dropped-sequence
events that naturally occur when multiple workers race to process the same
sensor concurrently would flood the output and add unrelated I/O overhead
to the timing.

## What each benchmark measures

- **`BenchmarkProcessor_WorkerCounts`** - enqueues messages from a single
  sensor as fast as possible, varying `NumWorkers` (1, 2, 4, 8, 16, 32),
  holding queue capacity fixed. Measures whether adding workers improves
  throughput.
- **`BenchmarkProcessor_SensorScaling`** - fixes worker count at 8 and
  varies the number of distinct sensors (10, 100, 500, 1000) sending
  traffic, isolating the cost of growing the sensor cardinality in the
  shared state store from pure worker-count effects.
- **`BenchmarkProcessor_Latency`** - reports p50/p95/p99 *end-to-end*
  latency (from a message's `Timestamp` to fully processed) as custom
  metrics, across worker counts.
- **`BenchmarkProcessor_Backpressure`** - hammers a single-worker processor
  with `TryEnqueue` faster than it can drain, across queue capacities (10,
  100, 1000), and reports how many messages were accepted vs. rejected once
  the queue filled.

## Test machine

- CPU: Intel(R) Core(TM) Ultra 7 155H (16 cores / 22 logical processors)
- RAM: ~32 GB
- OS/arch: `windows/amd64`
- Go: `go1.26.4`
- Command: `go test ./benchmarks/... -run=^$ -bench=. -benchtime=2s -benchmem`

## Results (2026-06-18, run as above)

```
goos: windows
goarch: amd64
pkg: sentinelstream/benchmarks
cpu: Intel(R) Core(TM) Ultra 7 155H
BenchmarkProcessor_WorkerCounts/workers=1-22         	  947773	      3470 ns/op	     456 B/op	       7 allocs/op
BenchmarkProcessor_WorkerCounts/workers=2-22         	  820524	      2887 ns/op	     461 B/op	       7 allocs/op
BenchmarkProcessor_WorkerCounts/workers=4-22         	  861762	      3260 ns/op	     470 B/op	       7 allocs/op
BenchmarkProcessor_WorkerCounts/workers=8-22         	  809276	      3446 ns/op	     472 B/op	       8 allocs/op
BenchmarkProcessor_WorkerCounts/workers=16-22        	  653259	      3855 ns/op	     472 B/op	       8 allocs/op
BenchmarkProcessor_WorkerCounts/workers=32-22        	  557324	      3774 ns/op	     472 B/op	       8 allocs/op
BenchmarkProcessor_SensorScaling/sensors=10-22       	  556990	      4686 ns/op	     472 B/op	       8 allocs/op
BenchmarkProcessor_SensorScaling/sensors=100-22      	  589062	      5174 ns/op	     475 B/op	       8 allocs/op
BenchmarkProcessor_SensorScaling/sensors=500-22      	  758864	      3940 ns/op	     480 B/op	       8 allocs/op
BenchmarkProcessor_SensorScaling/sensors=1000-22     	  919046	      3817 ns/op	     484 B/op	       8 allocs/op
BenchmarkProcessor_Latency/workers=1-22              	  649887	      4035 ns/op	     33239 p50_us	     36578 p95_us	     37198 p99_us	     456 B/op	       7 allocs/op
BenchmarkProcessor_Latency/workers=4-22              	  866460	      3527 ns/op	     40229 p50_us	     43227 p95_us	     43227 p99_us	     470 B/op	       7 allocs/op
BenchmarkProcessor_Latency/workers=8-22              	  602898	      3680 ns/op	     39879 p50_us	     44099 p95_us	     44107 p99_us	     472 B/op	       8 allocs/op
BenchmarkProcessor_Backpressure/queue=10-22          	 1496973	      1601 ns/op	    465834 accepted	   1031139 queue_full_events	   1031139 rejected	     304 B/op	       5 allocs/op
BenchmarkProcessor_Backpressure/queue=100-22         	 1499102	      1659 ns/op	    462565 accepted	   1036537 queue_full_events	   1036537 rejected	     303 B/op	       5 allocs/op
BenchmarkProcessor_Backpressure/queue=1000-22        	 1404266	      1640 ns/op	    463152 accepted	    941114 queue_full_events	    941114 rejected	     314 B/op	       6 allocs/op
PASS
ok  	sentinelstream/benchmarks	51.387s
```

## Interpretation

- **Worker count doesn't help a single-sensor workload.** `ns/op` is flat
  (~2.9-3.9 µs) from 1 to 32 workers, and if anything gets *slightly worse*
  at 16-32 workers. The reason: `StateStore` (`internal/state/state.go`)
  guards its entire sensor map with one `sync.RWMutex`, and every processed
  message calls `Update()`, which takes a write lock. With all messages
  targeting the same sensor ID, every worker serializes on that one mutex
  regardless of pool size - more workers just adds scheduling/context-switch
  overhead without adding parallelism. This is an accurate reflection of the
  current implementation, not a benchmark artifact: throughput here is
  bound by state-store lock contention, not by worker count. The README's
  own "Future Improvements" list names the fix - horizontal sharding by
  sensor ID - which would let independent sensors update without
  contending on the same lock.
- **Sensor cardinality (10 vs 1000 sensors) doesn't change much either, for
  the same reason:** the lock is global, not per-sensor, so spreading
  traffic across more sensors doesn't reduce contention in this
  implementation. The modest swings between sensor counts (3.8-5.2 µs) are
  consistent with run-to-run scheduling noise rather than a real effect.
- **End-to-end latency (tens of milliseconds) reflects queueing delay, not
  per-message processing cost.** These benchmarks intentionally enqueue as
  fast as the producer loop can go, well beyond what 1-8 workers can drain
  immediately. With `QueueSize: 10000`, messages queue up and `Timestamp`
  (recorded at message creation) accumulates real wait time before a worker
  reaches them - exactly the bounded-queue backpressure behavior the
  architecture is designed to exhibit under burst load. It is not a
  measurement of best-case single-message latency.
- **Backpressure is real and immediate under sustained max-rate load.**
  With a single worker and a producer that never blocks (`TryEnqueue`),
  roughly 45-69% of messages get rejected once the queue fills, across all
  three tested queue sizes (10/100/1000). A bigger queue absorbs more of a
  short burst but doesn't change the fundamental constraint: a single
  worker can't keep up with an unbounded-rate producer, so the bounded
  queue does its job of rejecting rather than accepting unboundedly.

## Known limitation surfaced by these results

`StateStore`'s single global mutex is the dominant bottleneck for
high-throughput, high-worker-count scenarios. It does not show up as a
correctness problem (the race detector is clean - see the main project
tests), only as a throughput ceiling. Sharding the store by sensor ID (e.g.
N buckets, each with its own mutex, sensor ID hashed to a bucket) is the
natural next step if higher throughput is ever needed; it's deliberately
not implemented here since the project's stated benchmark goal was to
*measure and document* this kind of bottleneck, not to pre-optimize before
having a number to justify it.
