package replay

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sentinelstream/internal/protocol"
)

// maxLineSize bounds how large a single recorded event line may be.
const maxLineSize = 1 << 20 // 1 MiB

// Player reads a JSON Lines event file and replays each message in order.
type Player struct {
	path string
}

// NewPlayer creates a player for the event file at path.
func NewPlayer(path string) *Player {
	return &Player{path: path}
}

// Replay reads every line from the file, parses it into a TelemetryMessage,
// and calls handle for each one in order. It stops at the first failure -
// the file can't be opened, a line fails to parse, handle returns an
// error, or ctx is cancelled - and returns the number of messages
// successfully handled before that point.
func (p *Player) Replay(ctx context.Context, handle func(*protocol.TelemetryMessage) error) (int, error) {
	f, err := os.Open(p.path)
	if err != nil {
		return 0, fmt.Errorf("failed to open replay file %s: %w", p.path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), maxLineSize)

	count := 0
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if err := ctx.Err(); err != nil {
			return count, err
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var msg protocol.TelemetryMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			return count, fmt.Errorf("failed to parse line %d: %w", lineNum, err)
		}

		if err := handle(&msg); err != nil {
			return count, fmt.Errorf("handler failed on line %d: %w", lineNum, err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading replay file: %w", err)
	}

	return count, nil
}
