package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/collector"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/output"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/processor"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to config file")
	filePath := flag.String("file", "", "Path to log file (adds to config paths)")
	esURLsStr := flag.String("es", "", "Comma-separated Elasticsearch URLs (overrides config)")
	numWorkers := flag.Int("workers", 0, "Number of processor workers (overrides config)")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config from %s: %v", *configPath, err)
	}

	// Override config with CLI flags if provided
	if *filePath != "" {
		cfg.Collector.Paths = append(cfg.Collector.Paths, *filePath)
	}
	if *esURLsStr != "" {
		cfg.Output.Elasticsearch.URLs = strings.Split(*esURLsStr, ",")
	}
	if *numWorkers > 0 {
		cfg.Processor.Workers = *numWorkers
	}


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

	// Initialize BookmarkManager
	bm, err := collector.NewBookmarkManager(cfg.Collector.BookmarkFile)
	if err != nil {
		log.Fatalf("Failed to initialize bookmark manager: %v", err)
	}

	// Start Collector Manager
	collectorManager := collector.NewManager(cfg.Collector.Paths, bm, rawLogs)
	go func() {
		defer close(rawLogs)
		collectorManager.Run(ctx)
	}()

	// Start Processor Pool
	workerWg := processor.StartWorkerPool(cfg.Processor, rawLogs, processedLogs)

	// Close processedLogs when all workers are done
	go func() {
		workerWg.Wait()
		close(processedLogs)
	}()

	// Start Output
	out, err := output.NewOutput(cfg.Output)
	if err != nil {
		log.Fatalf("Failed to initialize output: %v", err)
	}

	log.Println("Log aggregator started successfully. Press Ctrl+C to stop.")

	// Blocks until processedLogs is closed
	out.Run(processedLogs)

	// Flush bookmarks on exit
	if err := bm.Flush(); err != nil {
		log.Printf("Failed to flush bookmarks: %v", err)
	}

	log.Println("Shutdown complete.")
}
