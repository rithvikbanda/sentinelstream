// Package logging configures SentinelStream's structured JSON event log
// (accepted/rejected messages, sensor health transitions, backpressure,
// connection lifecycle, etc).
package logging

import (
	"io"
	"log/slog"
)

// Init configures the process-wide default slog.Logger to emit JSON lines
// to w, renaming the standard "time"/"msg" keys to "timestamp"/"event" to
// match SentinelStream's event schema, e.g.:
//
//	{"level":"warn","event":"sensor_stale","sensor_id":"drone-17","timestamp":"..."}
func Init(w io.Writer) *slog.Logger {
	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				a.Key = "timestamp"
			case slog.MessageKey:
				a.Key = "event"
			}
			return a
		},
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}
