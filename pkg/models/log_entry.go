package models

import (
	"encoding/json"
	"time"
)

type LogEntry struct {
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level,omitempty"`
	RequestID string            `json:"request_id,omitempty"`
	Source    string            `json:"source"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`

	FlattenLabels bool `json:"-"`
}

func (l LogEntry) MarshalJSON() ([]byte, error) {
	if !l.FlattenLabels || len(l.Labels) == 0 {
		type Alias LogEntry
		return json.Marshal(&struct {
			*Alias
		}{
			Alias: (*Alias)(&l),
		})
	}

	m := make(map[string]interface{})
	m["timestamp"] = l.Timestamp
	if l.Level != "" {
		m["level"] = l.Level
	}
	if l.RequestID != "" {
		m["request_id"] = l.RequestID
	}
	m["source"] = l.Source
	m["message"] = l.Message

	for k, v := range l.Labels {
		m[k] = v
	}

	return json.Marshal(m)
}
