# SentinelStream

SentinelStream is a concurrent telemetry ingestion and monitoring service
written in Go. Simulated sensors (GPS trackers, drones, vehicles,
temperature sensors) send real-time telemetry over UDP or TCP; a bounded
worker pool validates and processes it; a background monitor detects stale
or anomalous sensors; and a REST API exposes live sensor state and system
metrics.

It's a learning/portfolio project demonstrating Go concurrency
(goroutines, channels, worker pools), networking (TCP/UDP servers),
backpressure handling, structured logging, and benchmarking - built in
phases, each with its own tests and a real `go test -race` pass before
moving on.

## Architecture

```text
┌──────────────────────┐
│   Sensor Simulator    │  cmd/simulator - generates GPS/drone/vehicle/
│  (--target host:port) │  temperature telemetry, optionally sends it over
└──────────┬────────────┘  UDP or TCP to a running server
           │
       TCP / UDP
           │
┌──────────▼────────────┐
│   Network Listeners    │  internal/ingestion
│ UDP datagram loop       │  - one packet = one message (UDP)
│ TCP: one goroutine/conn │  - newline-delimited JSON (TCP)
└──────────┬────────────┘
           │
┌──────────▼────────────┐
│ Parser & Envelope       │  internal/protocol
│ Validation              │  required fields, timestamp sanity, payload size
└──────────┬────────────┘
           │
┌──────────▼────────────┐
│ Bounded Channel         │  internal/processor
│ (backpressure)          │  chan *TelemetryMessage, fixed capacity
└──────────┬────────────┘
           │
┌──────────▼────────────┐
│ Worker Pool             │  internal/processor
│ - sensor-specific        │  - per-sensor range validation -> anomalies
│   range validation       │  - sequence tracking -> dropped/duplicate/
│ - sequence tracking       │    out-of-order
│ - latency recording       │
└───────┬─────────┬─────┘
        │         │
┌───────▼───┐  ┌──▼─────────────────┐
│State Store│  │ Stale Monitor        │  internal/state, internal/health
│(RWMutex)  │  │ (background scan)    │
└───────┬───┘  └──────────┬──────────┘
        │                 │
┌───────▼─────────────────▼────────┐
│ REST API (internal/api)            │
│ /api/v1/sensors                    │
│ /api/v1/sensors/{id}                │
│ /api/v1/health/streams              │
│ /api/v1/metrics/summary             │
└─────────────────────────────────────┘

Optional, alongside the pipeline:
  internal/replay   - record accepted messages to JSON Lines, replay later
  internal/logging  - structured JSON event log (log/slog)
```

## Quick start

### Locally

```bash
go build -o bin/server ./cmd/server
go build -o bin/simulator ./cmd/simulator
./bin/server &
./bin/simulator --target 127.0.0.1:9000 --protocol udp --sensors 20 --rate 5
curl http://localhost:8080/api/v1/sensors
```

### Dashboard (optional)

A small Node/Express app (`ui/`) serves a static page that polls the REST
API directly from the browser - no build step, no framework:

```bash
cd ui
npm install
npm start          # http://localhost:3000
```

It defaults to `http://localhost:8080` for the API; change it in the input
field at the top of the page (saved to `localStorage`) if your server is
running elsewhere. The Go API sends permissive CORS headers
(`Access-Control-Allow-Origin: *`) specifically so this works without a
proxy - fine for a local demo, would need narrowing for any real
deployment.

### Docker Compose

```bash
docker compose up --build
```

This builds one image (containing the `server`, `simulator`, and `replay`
binaries) and starts two containers: `server` (UDP 9000, TCP 9001, REST API
on 8080, all published to the host) and `simulator` (configured with
`--target=server:9000`, sending real UDP traffic to the server over the
Docker network using Compose's built-in service-name DNS). Watch it work:

```bash
curl http://localhost:8080/api/v1/metrics/summary
curl http://localhost:8080/api/v1/sensors
```

### Makefile shortcuts

```bash
make build       # build all three binaries into bin/
make test        # go test ./...
make race        # go test -race ./...
make bench       # run the benchmarks (see benchmarks/README.md)
make docker-up   # docker compose up --build
```

## Configuration

All three binaries (`server`, `simulator`, `replay`) are configured purely
through CLI flags - there is no config file or env var layer (see [Known
limitations](#known-limitations)). `docker-compose.yml` doubles as a
runnable sample configuration; the flags below are the full reference.

### `cmd/server`

| Flag | Default | Meaning |
|---|---|---|
| `--port` | `9000` | UDP port to listen on |
| `--tcp-port` | `9001` | TCP port to listen on |
| `--api-addr` | `:8080` | REST API listen address |
| `--buffer` | `1000` | Bounded queue capacity (see [Backpressure policy](#backpressure-policy)) |
| `--stale-timeout` | `5s` | Time without a message before a sensor is marked stale |
| `--health-check-interval` | `1s` | How often the stale-sensor scan runs |
| `--record` | _(disabled)_ | If set, append every accepted message to this JSON Lines file |

### `cmd/simulator`

| Flag | Default | Meaning |
|---|---|---|
| `--sensors` | `5` | Number of sensors to simulate **per type** |
| `--rate` | `10` | Messages per second per sensor |
| `--types` | `gps,drone,vehicle,temperature` | Comma-separated sensor types |
| `--verbose` | `true` | Print every generated message |
| `--protocol` | `udp` | `udp` or `tcp`, used when `--target` is set |
| `--target` | _(disabled)_ | `host:port` of a server to actually send to; if empty, the simulator only prints locally |

### `cmd/replay`

| Flag | Default | Meaning |
|---|---|---|
| `--file` | `events.jsonl` | JSON Lines file to replay |
| `--workers` | `4` | Worker pool size for the replay's own in-process processor |
| `--queue` | `1000` | Queue capacity for the replay's own in-process processor |

## Backpressure policy

The ingestion queue (`internal/processor`) is a fixed-capacity Go channel.
Two ways to submit to it, with different policies, both implemented and
used by design rather than picking one and hard-coding it everywhere:

- **`Enqueue` (block)** - used by the UDP/TCP listener forwarding
  goroutines in `cmd/server`. If the queue is full, the call blocks until a
  worker frees a slot. This is correct for those goroutines because
  blocking only slows that one connection/listener loop down; it doesn't
  drop data, and UDP/TCP read loops can tolerate brief stalls.
- **`TryEnqueue` (reject)** - a non-blocking alternative that returns an
  error immediately if the queue is full, incrementing a `queue_full`
  counter and emitting a `queue_full` structured log event. This is the
  right choice for a caller that must never block (e.g. a request handler),
  and is what the backpressure benchmark in `benchmarks/` uses to measure
  rejection behavior under sustained burst load.

Either way, the queue's bounded capacity is what prevents unbounded memory
growth during a traffic burst - see `benchmarks/README.md` for measured
accept/reject rates at different queue sizes.

## REST API

```bash
# List all known sensors
curl http://localhost:8080/api/v1/sensors

# One sensor's detail (404 if unknown)
curl http://localhost:8080/api/v1/sensors/gps-0

# UDP/TCP listener + queue health
curl http://localhost:8080/api/v1/health/streams

# System-wide counters
curl http://localhost:8080/api/v1/metrics/summary
```

Example `/api/v1/sensors/{id}` response:

```json
{
  "sensor_id": "drone-1",
  "sensor_type": "drone",
  "status": "healthy",
  "last_sequence": 42,
  "messages_received": 42,
  "messages_dropped": 0,
  "duplicates": 0,
  "out_of_order": 0,
  "errors": 0,
  "anomalies": 1,
  "last_seen": "2026-06-18T20:00:03.69Z"
}
```

`POST /api/v1/replay` is intentionally not implemented - per the original
design, replay stays a CLI-only feature (`cmd/replay`) for this version.

## Structured logging & replay

Every server/replay run emits JSON event logs (via `log/slog`) for the
events that matter operationally: `validation_failed`, `connection_opened`/
`connection_closed`, `sequence_dropped`, `duplicate_message`,
`out_of_order`, `anomaly_detected`, `processing_error`, `queue_full`,
`sensor_stale`, `replay_started`, `server_shutdown`. Example:

```json
{"timestamp":"2026-06-18T19:43:37Z","level":"WARN","event":"sensor_stale","sensor_id":"drone-17","timeout_ms":5000}
```

Pass `--record events.jsonl` to `cmd/server` to additionally persist every
*accepted* message as one JSON line per message. Replay it later through
the exact same processing pipeline:

```bash
./bin/replay --file events.jsonl
```

## Testing

```bash
go test ./...                  # full suite
go test -race ./...            # with the race detector (requires a C toolchain - cgo)
go test ./benchmarks/... -run=^$ -bench=. -benchmem   # benchmarks
```

Every package added in each development phase has its own unit tests; the
state store, processor, and replay pipeline additionally have dedicated
concurrency/integration tests verifying behavior under the race detector.

## Benchmarks

See [benchmarks/README.md](benchmarks/README.md) for full methodology,
exact commands, machine spec, and results. Headline finding: worker count
and sensor cardinality have little effect on throughput in the current
implementation, because `StateStore` guards its entire sensor map with one
`sync.RWMutex` - every processed message serializes on that lock regardless
of pool size. See [Known limitations](#known-limitations).

## Design decisions

- **Bounded channel, not an unbounded queue or external broker.** A plain
  `chan *TelemetryMessage` with a fixed capacity was enough to demonstrate
  backpressure without pulling in Kafka/NATS (listed only as a possible
  future improvement).
- **UDP and TCP share one pipeline.** Both listeners parse into the same
  `protocol.TelemetryMessage` and feed the same `Processor`, so all
  downstream logic (validation, sequence tracking, state, API) is
  protocol-agnostic.
- **Anomalies vs. errors are distinguished by error type, not duplicated
  logic.** A worker tells a sensor-range violation (e.g. battery > 100)
  apart from a genuine processing failure using `errors.As` against the
  existing `protocol.ValidationError` type, rather than re-implementing
  range checks in two places.
- **`log/slog`'s JSON handler over a custom logger.** The standard library
  already does structured JSON logging well (Go 1.21+); a `ReplaceAttr`
  hook renames `time`/`msg` to `timestamp`/`event` to match this project's
  event schema, instead of writing a logging package from scratch.
  Console-facing output (startup banners, periodic stats) deliberately
  stays plain `fmt.Printf` - it's a dashboard for a human, not a discrete
  event.
- **No third-party dependencies.** `go.mod` lists none. Percentile latency
  tracking (`internal/metrics`), JSON Lines replay, and the REST router
  (Go 1.22+'s enhanced `http.ServeMux` method+path patterns) are all
  implemented with the standard library only.
- **Replay rebuilds its own in-process pipeline rather than talking to a
  live server.** `cmd/replay` constructs a fresh `state.StateStore` +
  `processor.Processor` and feeds recorded messages directly into
  `proc.Enqueue` - literally "the same processing pipeline" the spec calls
  for, testable without a running server, and exactly what the replay
  integration test exercises.

## Known limitations

- **`StateStore` uses one global `sync.RWMutex` for the whole sensor map.**
  Correct (race-detector clean) but not lock-sharded, so it becomes the
  throughput ceiling under high concurrency - see Benchmarks above. The
  natural fix, sharding by sensor ID, is deliberately left undone until
  there's a measured reason to do it.
- **No config file or environment variable layer.** Everything is a CLI
  flag; `internal/config` exists as an empty placeholder package only.
  `docker-compose.yml`'s `command:` blocks are the closest thing to a
  config file today.
- **No persistent storage.** Sensor state and recorded events live in
  memory / a local JSONL file; a restart loses all sensor state (recorded
  replay files survive, since they're just files).
- **No authentication on the REST API or the UDP/TCP listeners.** Anyone
  who can reach the configured ports can submit telemetry or read sensor
  state. Fine for a local demo; would need addressing before any real
  deployment.
- **The REST API sends `Access-Control-Allow-Origin: *`** so the optional
  `ui/` dashboard can call it from a different origin without a proxy.
  Fine for local use; a real deployment should scope this to a specific
  origin instead of allowing any.
- **The simulator's network sender doesn't retry or reconnect.** If
  `Send` fails (e.g. the server isn't up yet), the simulator logs the error
  and keeps generating messages, but a dropped TCP connection is not
  re-established. Acceptable for a demo/load-generation tool; `depends_on`
  in `docker-compose.yml` only orders container startup, it doesn't wait
  for the server's listener to actually be ready.

## Possible future improvements

PostgreSQL/Redis persistence, Prometheus metrics, Grafana dashboards, a
gRPC API, Kafka/NATS ingestion, Kubernetes deployment, and horizontal
sharding by sensor ID are all reasonable next steps, intentionally not
built here - see Known limitations for which of these the current
benchmarks actually justify.
