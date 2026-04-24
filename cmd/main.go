package main

import (
	"log"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/collector"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/output"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/processor"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func main() {
	rawLogs := make(chan models.LogEntry, 100)
	processedLogs := make(chan models.LogEntry, 100)

	go func() {
		err := collector.CollectLogs("app.log", rawLogs)
		if err != nil {
			log.Fatal(err)
		}
	}()

	numWorkers := 2
	processor.StartWorkerPool(numWorkers, rawLogs, processedLogs)

	esOutput, err := output.NewElasticsearchOutput(
		"logs-index",
		10,
		5*time.Second,
	)
	if err != nil {
		log.Fatal(err)
	}
	esOutput.Run(processedLogs)
	//output.WriteLogs(processedLogs)
}
