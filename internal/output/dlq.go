package output

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/metrics"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

// DLQ (Dead Letter Queue) persists failed log entries to a local JSONL file
// so they can be inspected or retried later.
type DLQ struct {
	mu   sync.Mutex
	file *os.File
	path string
}

// NewDLQ creates a new Dead Letter Queue that appends to the given file path.
func NewDLQ(path string) (*DLQ, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open DLQ file %s: %w", path, err)
	}

	slog.Info("Dead Letter Queue initialized", "path", path)
	return &DLQ{file: f, path: path}, nil
}

// Write persists a failed log entry along with the reason for failure.
func (d *DLQ) Write(entry models.LogEntry, reason string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	record := struct {
		Reason string          `json:"dlq_reason"`
		Entry  models.LogEntry `json:"entry"`
	}{
		Reason: reason,
		Entry:  entry,
	}

	data, err := json.Marshal(record)
	if err != nil {
		slog.Error("DLQ: failed to marshal entry", "error", err)
		return
	}

	data = append(data, '\n')
	if _, err := d.file.Write(data); err != nil {
		slog.Error("DLQ: failed to write entry", "error", err)
		return
	}

	metrics.Global.LogsSentToDLQ.Add(1)
}

// Close closes the underlying DLQ file.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.file.Close()
}
