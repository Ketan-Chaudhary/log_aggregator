package processor

import (
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func TestWorker(t *testing.T) {
	tests := []struct {
		name     string
		runtime  *ProcessorRuntime
		input    models.LogEntry
		wantEmit bool
		wantLv   string
		wantLab  map[string]string
	}{
		{
			name: "Passes severity filter (INFO >= INFO)",
			runtime: &ProcessorRuntime{
				MinSeverity: 2, // INFO
			},
			input:    models.LogEntry{Message: `{"level":"INFO","msg":"test"}`},
			wantEmit: true,
			wantLv:   "INFO",
		},
		{
			name: "Drops below severity filter (DEBUG < INFO)",
			runtime: &ProcessorRuntime{
				MinSeverity: 2, // INFO
			},
			input:    models.LogEntry{Message: `{"level":"DEBUG","msg":"test"}`},
			wantEmit: false,
		},
		{
			name: "Handles unknown level (UNKNOWN < INFO drops)",
			runtime: &ProcessorRuntime{
				MinSeverity: 2, // INFO
			},
			input:    models.LogEntry{Message: `{"level":"WEIRD","msg":"test"}`},
			wantEmit: false,
		},
		{
			name: "Raw regex drop matches",
			runtime: &ProcessorRuntime{
				DropRegexes: []*regexp.Regexp{regexp.MustCompile(`healthcheck`)},
			},
			input:    models.LogEntry{Message: `{"endpoint":"healthcheck"}`},
			wantEmit: false,
		},
		{
			name: "Raw regex drop does not match",
			runtime: &ProcessorRuntime{
				DropRegexes: []*regexp.Regexp{regexp.MustCompile(`healthcheck`)},
			},
			input:    models.LogEntry{Message: `{"endpoint":"users"}`},
			wantEmit: true,
		},
		{
			name: "Enriches labels",
			runtime: &ProcessorRuntime{
				Labels: map[string]string{"env": "prod"},
			},
			input:    models.LogEntry{Message: `{"level":"INFO"}`},
			wantEmit: true,
			wantLab:  map[string]string{"env": "prod"},
		},
		{
			name: "Malformed JSON passes with UNKNOWN level (if MinSeverity is 0)",
			runtime: &ProcessorRuntime{
				MinSeverity: 0,
			},
			input:    models.LogEntry{Message: `this is not json`},
			wantEmit: true,
			wantLv:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make(chan models.LogEntry, 1)
			out := make(chan models.LogEntry, 1)

			in <- tt.input
			close(in)

			var wg sync.WaitGroup
			wg.Add(1)
			go Worker(&wg, tt.runtime, in, out)

			wg.Wait()
			close(out)

			var emitted []models.LogEntry
			for log := range out {
				emitted = append(emitted, log)
			}

			if tt.wantEmit && len(emitted) == 0 {
				t.Fatalf("expected log to be emitted, but it was dropped")
			}
			if !tt.wantEmit && len(emitted) > 0 {
				t.Fatalf("expected log to be dropped, but it was emitted")
			}

			if tt.wantEmit {
				log := emitted[0]
				if tt.wantLv != "" && log.Level != tt.wantLv {
					t.Errorf("got level %s, want %s", log.Level, tt.wantLv)
				}
				if tt.wantLab != nil {
					for k, wantV := range tt.wantLab {
						if gotV, ok := log.Labels[k]; !ok || gotV != wantV {
							t.Errorf("got label %s=%s, want %s", k, gotV, wantV)
						}
					}
				}
				// ensure timestamp is set
				if log.Timestamp.IsZero() || log.Timestamp.Unix() < time.Now().Add(-1*time.Minute).Unix() {
					t.Errorf("timestamp was not set or is suspiciously old: %v", log.Timestamp)
				}
			}
		})
	}
}
