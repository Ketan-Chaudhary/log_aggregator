package processor

import "github.com/Ketan-Chaudhary/log_aggregator/pkg/models"

func StartWorkerPool(numWorkers int, in <-chan models.LogEntry, out chan<- models.LogEntry) {
	for i := 0; i < numWorkers; i++ {
		go Worker(i, in, out)
	}
}
