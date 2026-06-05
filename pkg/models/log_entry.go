package models

import "time"

type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     string    `json:"level,omitempty"`
	RequestID string    `json:"request_id,omitempty"`
	Source    string    `json:"source"`
	Message   string    `json:"message"`
}
