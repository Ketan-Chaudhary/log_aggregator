package logger

import (
	"log/slog"
	"os"
	"strings"
)

// Setup initializes the global slog logger with the given level string.
// Valid levels: DEBUG, INFO, WARN, ERROR. Defaults to INFO.
func Setup(levelStr string) {
	var level slog.Level
	switch strings.ToUpper(strings.TrimSpace(levelStr)) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN", "WARNING":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	slog.SetDefault(slog.New(handler))
}
