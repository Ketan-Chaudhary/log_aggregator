package processor

import (
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func ProcessLogs(in <-chan models.LogEntry, out chan<- models.LogEntry) {
	for log := range in {
		log.Timestamp = time.Now()
		log.Source = "app-log"

		out <- log
	}
}
