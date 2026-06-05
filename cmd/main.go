package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/collector"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/output"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/processor"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func main() {
	var (
		filePath   string
		esURLsStr  string
		numWorkers int
	)
	flag.StringVar(&filePath, "file", "app.log", "Path to log file")
	flag.StringVar(&esURLsStr, "es", "http://127.0.0.1:9200", "Comma-separated Elasticsearch URLs")
	flag.IntVar(&numWorkers, "workers", 2, "Number of processor workers")
	flag.Parse()

	esURLs := strings.Split(esURLsStr, ",")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received termination signal, shutting down...")
		cancel()
	}()

	rawLogs := make(chan models.LogEntry, 100)
	processedLogs := make(chan models.LogEntry, 100)

	// Start Collector
	go func() {
		defer close(rawLogs)
		err := collector.CollectLogs(ctx, filePath, rawLogs)
		if err != nil && err != context.Canceled {
			log.Printf("Collector error: %v", err)
		}
	}()

	// Start Processor Pool
	workerWg := processor.StartWorkerPool(numWorkers, rawLogs, processedLogs)
	
	// Close processedLogs when all workers are done
	go func() {
		workerWg.Wait()
		close(processedLogs)
	}()

	// Start Output
	esOutput, err := output.NewElasticsearchOutput(
		esURLs,
		"logs-index",
		10,
		5*time.Second,
	)
	if err != nil {
		log.Fatal("Failed to initialize ES output: ", err)
	}
	
	log.Println("Log aggregator started successfully. Press Ctrl+C to stop.")
	
	// This will block until processedLogs is closed, which happens after workers finish,
	// which happens after rawLogs is closed, which happens after collector stops.
	esOutput.Run(processedLogs)

	log.Println("Shutdown complete.")
}
