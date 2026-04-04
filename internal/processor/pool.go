package processor

import (
	"fmt"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func StartWorkerPool(numWorkers int, in <-chan models.LogEntry, out chan<- models.LogEntry) {
	if numWorkers < 1 {
		fmt.Println("Invalid Worker count: defaulting to 1")
		numWorkers = 1
	}
	for i := 0; i < numWorkers; i++ {
		go Worker(i, in, out)
	}
}
