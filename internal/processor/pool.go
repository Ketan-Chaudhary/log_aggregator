package processor

import (
	"fmt"
	"sync"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func StartWorkerPool(numWorkers int, in <-chan models.LogEntry, out chan<- models.LogEntry) *sync.WaitGroup {
	if numWorkers < 1 {
		fmt.Println("Invalid Worker count: defaulting to 1")
		numWorkers = 1
	}
	
	wg := &sync.WaitGroup{}
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go Worker(wg, in, out)
	}
	return wg
}
