package processor

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/metrics"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func Worker(wg *sync.WaitGroup, runtime *ProcessorRuntime, in <-chan models.LogEntry, out chan<- models.LogEntry) {
	defer wg.Done()
	for log := range in {
		metrics.Global.LogsReceived.Add(1)
		// 1. Raw Drop Filter
		dropped := false
		if len(runtime.DropRegexes) > 0 {
			for _, re := range runtime.DropRegexes {
				if re.MatchString(log.Message) {
					dropped = true
					break
				}
			}
		}
		if dropped {
			metrics.Global.LogsDroppedRegex.Add(1)
			continue
		}

		// 2. Parse / Extract
		log.Timestamp = time.Now() // default fallback

		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(log.Message), &raw); err == nil {
			if tsStr, ok := raw["timestamp"].(string); ok {
				if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
					log.Timestamp = t
				}
			}
			if lvl, ok := raw["level"].(string); ok {
				log.Level = lvl
			}
			if reqID, ok := raw["request_id"].(string); ok {
				log.RequestID = reqID
			}
		} else {
			// Fallback to Regex Parsing
			for _, re := range runtime.ExtractRegexes {
				match := re.FindStringSubmatch(log.Message)
				if match != nil {
					for i, name := range re.SubexpNames() {
						if i != 0 && name != "" {
							val := match[i]
							switch name {
							case "level":
								log.Level = val
							case "request_id":
								log.RequestID = val
							case "timestamp":
								if t, err := time.Parse(time.RFC3339, val); err == nil {
									log.Timestamp = t
								}
							}
						}
					}
					break // Stop after first regex match
				}
			}
		}

		// 3. Severity Filter
		// If MinSeverity is greater than 0, we enforce dropping.
		// If MinLevel was not set in config, MinSeverity will be 0, so nothing drops.
		if runtime.MinSeverity > 0 {
			sev := getSeverity(log.Level)
			if sev < runtime.MinSeverity {
				metrics.Global.LogsDroppedSeverity.Add(1)
				continue
			}
		}

		// 4. Enrichment
		if len(runtime.Labels) > 0 {
			log.Labels = make(map[string]string, len(runtime.Labels))
			for k, v := range runtime.Labels {
				log.Labels[k] = v
			}
		}
		log.FlattenLabels = runtime.FlattenLabels

		// 5. Output
		metrics.Global.LogsFlushedToOutput.Add(1)
		out <- log
	}
}
