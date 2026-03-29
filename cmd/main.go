package main

import (
	"log"

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

	go processor.ProcessLogs(rawLogs, processedLogs)

	output.WriteLogs(processedLogs)
}
