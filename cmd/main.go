package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/collector"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/logger"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/output"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/processor"
	"github.com/Ketan-Chaudhary/log_aggregator/internal/server"
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

	// Initialize structured logger
	logger.Setup(cfg.LogLevel)

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
		slog.Info("Received termination signal, shutting down...")
		cancel()
	}()

	// Initialize Dead Letter Queue
	dlq, err := output.NewDLQ(cfg.DLQPath)
	if err != nil {
		slog.Error("Failed to initialize DLQ", "error", err)
		os.Exit(1)
	}
	defer dlq.Close()

	// Start Stats & Health HTTP Server
	statsServer := server.New(fmt.Sprintf(":%d", cfg.StatsPort))
	statsServer.Start(ctx)

	rawLogs := make(chan models.LogEntry, cfg.BufferSize)
	processedLogs := make(chan models.LogEntry, cfg.BufferSize)

	// Initialize BookmarkManager
	bm, err := collector.NewBookmarkManager(cfg.Collector.BookmarkFile)
	if err != nil {
		slog.Error("Failed to initialize bookmark manager", "error", err)
		os.Exit(1)
	}

	// Start periodic bookmark flushing (every 5 seconds, final flush on shutdown)
	go bm.StartPeriodicFlush(ctx, 5*time.Second)

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
	out, err := output.NewOutput(cfg.Output, dlq)
	if err != nil {
		slog.Error("Failed to initialize output", "error", err)
		os.Exit(1)
	}

	// Mark service as ready
	statsServer.SetReady(true)

	slog.Info("Log aggregator started successfully. Press Ctrl+C to stop.")

	// Blocks until processedLogs is closed
	out.Run(ctx, processedLogs)

	slog.Info("Shutdown complete.")
}
