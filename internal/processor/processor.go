package processor

import (
	"fmt"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func Worker(id int, in <-chan models.LogEntry, out chan<- models.LogEntry) {
	for log := range in {
		log.Timestamp = time.Now()
		log.Source = fmt.Sprintf("worker-%d", id)

		out <- log
	}
}
