package processor

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func Worker(wg *sync.WaitGroup, in <-chan models.LogEntry, out chan<- models.LogEntry) {
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
		}

		out <- log
	}
}
