package benchmarks

import (
	"io"
	"log/slog"
	"testing"
)

// TestMain silences the structured event log for benchmark runs. Multiple
// workers racing to process the same sensor concurrently is expected to
// produce out-of-order/dropped-sequence events (Phase 5 behavior, not a
// bug), and logging each one would both flood benchmark output and add I/O
// overhead that has nothing to do with what these benchmarks measure.
func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.Run()
}
