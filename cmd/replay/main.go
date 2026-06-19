package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sentinelstream/internal/logging"
	"sentinelstream/internal/processor"
	"sentinelstream/internal/replay"
	"sentinelstream/internal/state"
	"time"
)

func main() {
	file := flag.String("file", "events.jsonl", "JSON Lines file of recorded telemetry events to replay")
	workers := flag.Int("workers", 4, "number of worker goroutines to process replayed events")
	queueSize := flag.Int("queue", 1000, "size of the processing queue")

	flag.Parse()

	logging.Init(os.Stdout)

	fmt.Printf("=== SentinelStream Replay ===\n")
	fmt.Printf("Replaying events from %s\n\n", *file)
	slog.Info("replay_started", "file", *file)

	store := state.NewStateStore()
	proc := processor.NewProcessor(processor.ProcessorConfig{
		NumWorkers: *workers,
		QueueSize:  *queueSize,
	}, store)

	if err := proc.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start processor: %v\n", err)
		os.Exit(1)
	}
	defer proc.Stop()

	player := replay.NewPlayer(*file)
	count, err := player.Replay(context.Background(), proc.Enqueue)
	if err != nil {
		fmt.Fprintf(os.Stderr, "replay stopped after %d event(s): %v\n", count, err)
		os.Exit(1)
	}

	if err := proc.WaitForQuiescence(10 * time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

	stats := proc.Stats()
	fmt.Printf("Replayed %d event(s)\n", count)
	fmt.Printf("Processor: %s\n", stats.String())
	fmt.Printf("Unique sensors: %d\n", store.Count())
}
