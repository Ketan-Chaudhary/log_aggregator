package metrics

import (
	"sync/atomic"
	"time"
)

// Stats holds all pipeline metrics using atomic counters for thread safety.
type Stats struct {
	LogsReceived        atomic.Int64
	LogsDroppedSeverity atomic.Int64
	LogsDroppedRegex    atomic.Int64
	LogsFlushedToOutput atomic.Int64
	LogsSentToDLQ       atomic.Int64
	ESFlushErrors       atomic.Int64
	ActiveFiles         atomic.Int64
	StartTime           time.Time
}

// Global is the singleton metrics instance used across the application.
var Global = &Stats{
	StartTime: time.Now(),
}

// Snapshot returns a JSON-serializable point-in-time view of all metrics.
type Snapshot struct {
	LogsReceived        int64   `json:"logs_received"`
	LogsDroppedSeverity int64   `json:"logs_dropped_severity"`
	LogsDroppedRegex    int64   `json:"logs_dropped_regex"`
	LogsFlushedToOutput int64   `json:"logs_flushed_to_output"`
	LogsSentToDLQ       int64   `json:"logs_sent_to_dlq"`
	ESFlushErrors       int64   `json:"es_flush_errors"`
	ActiveFiles         int64   `json:"active_files"`
	UptimeSeconds       float64 `json:"uptime_seconds"`
}

func (s *Stats) Snapshot() Snapshot {
	return Snapshot{
		LogsReceived:        s.LogsReceived.Load(),
		LogsDroppedSeverity: s.LogsDroppedSeverity.Load(),
		LogsDroppedRegex:    s.LogsDroppedRegex.Load(),
		LogsFlushedToOutput: s.LogsFlushedToOutput.Load(),
		LogsSentToDLQ:       s.LogsSentToDLQ.Load(),
		ESFlushErrors:       s.ESFlushErrors.Load(),
		ActiveFiles:         s.ActiveFiles.Load(),
		UptimeSeconds:       time.Since(s.StartTime).Seconds(),
	}
}
