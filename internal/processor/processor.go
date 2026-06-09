package processor

import (
	"encoding/json"
	"regexp"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func Worker(wg *sync.WaitGroup, regexes []*regexp.Regexp, in <-chan models.LogEntry, out chan<- models.LogEntry) {
	defer wg.Done()
	for log := range in {
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
			for _, re := range regexes {
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

		out <- log
	}
}
