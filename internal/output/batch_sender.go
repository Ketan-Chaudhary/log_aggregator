package output

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/metrics"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

// BulkFlusher is the interface that backend-specific outputs must implement.
// It performs the actual HTTP bulk request and returns granular results.
type BulkFlusher interface {
	// FlushBulk sends a batch of logs to the backend.
	// Returns:
	//   retryable  — entries that failed with a retryable status (429, 5xx)
	//   dropped    — count of entries that failed with a non-retryable status (sent to DLQ)
	//   err        — a transport-level error (network failure, timeout)
	FlushBulk(ctx context.Context, logs []models.LogEntry) (retryable []models.LogEntry, dropped int, err error)

	// Name returns a human-readable name for log messages (e.g. "Elasticsearch", "OpenSearch").
	Name() string
}

// BatchSender collects log entries into batches and flushes them through a
// BulkFlusher. It owns the batching loop, sender worker pool, retry logic
// with exponential backoff, and DLQ routing.
type BatchSender struct {
	flusher     BulkFlusher
	batchSize   int
	flushPeriod time.Duration
	dlq         *DLQ
	numWorkers  int

	sendQueue chan []models.LogEntry
	wg        sync.WaitGroup
}

// BatchSenderConfig holds the parameters needed to create a BatchSender.
type BatchSenderConfig struct {
	BatchSize   int
	FlushPeriod time.Duration
	NumWorkers  int
	DLQ         *DLQ
	Flusher     BulkFlusher
}

// NewBatchSender creates a BatchSender and starts its sender worker goroutines.
func NewBatchSender(cfg BatchSenderConfig) *BatchSender {
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = 3
	}
	bs := &BatchSender{
		flusher:     cfg.Flusher,
		batchSize:   cfg.BatchSize,
		flushPeriod: cfg.FlushPeriod,
		dlq:         cfg.DLQ,
		numWorkers:  cfg.NumWorkers,
		sendQueue:   make(chan []models.LogEntry, 100),
	}
	for i := 0; i < bs.numWorkers; i++ {
		bs.wg.Add(1)
		go bs.senderWorker(i)
	}
	return bs
}

// Run reads from the input channel, batches entries, and dispatches them to
// the sender workers. It respects context cancellation for graceful shutdown.
func (bs *BatchSender) Run(ctx context.Context, in <-chan models.LogEntry) {
	ticker := time.NewTicker(bs.flushPeriod)
	defer ticker.Stop()

	batch := make([]models.LogEntry, 0, bs.batchSize)

	for {
		select {
		case <-ctx.Done():
			// Drain remaining entries from the channel before shutting down.
			for {
				select {
				case entry, ok := <-in:
					if !ok {
						goto done
					}
					batch = append(batch, entry)
				default:
					goto done
				}
			}
		done:
			if len(batch) > 0 {
				bs.sendQueue <- append([]models.LogEntry(nil), batch...)
			}
			close(bs.sendQueue)
			bs.wg.Wait()
			return

		case entry, ok := <-in:
			if !ok {
				if len(batch) > 0 {
					bs.sendQueue <- append([]models.LogEntry(nil), batch...)
				}
				close(bs.sendQueue)
				bs.wg.Wait()
				return
			}
			batch = append(batch, entry)
			if len(batch) >= bs.batchSize {
				bs.sendQueue <- append([]models.LogEntry(nil), batch...)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				bs.sendQueue <- append([]models.LogEntry(nil), batch...)
				batch = batch[:0]
			}
		}
	}
}

func (bs *BatchSender) senderWorker(id int) {
	defer bs.wg.Done()
	name := bs.flusher.Name()
	slog.Debug("sender worker started", "backend", name, "worker_id", id)

	for batch := range bs.sendQueue {
		slog.Debug("worker processing batch",
			"backend", name,
			"worker_id", id,
			"batch_size", len(batch),
		)
		bs.flushWithRetry(batch)
	}
}

func (bs *BatchSender) flushWithRetry(logs []models.LogEntry) {
	if len(logs) == 0 {
		return
	}

	name := bs.flusher.Name()
	maxRetries := 3
	baseDelay := 500 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		retryable, dropped, err := bs.flusher.FlushBulk(ctx, logs)
		cancel()

		// Complete success
		if err == nil && len(retryable) == 0 {
			successCount := len(logs) - dropped
			if successCount > 0 {
				slog.Info("successfully flushed batch",
					"backend", name,
					"count", successCount,
				)
			}
			if dropped > 0 {
				slog.Warn("permanently dropped documents with non-retryable failures",
					"backend", name,
					"dropped", dropped,
				)
			}
			return
		}

		// Transport-level error (network failure, timeout)
		if err != nil {
			slog.Error("bulk request failed",
				"backend", name,
				"error", err,
				"attempt", attempt,
			)

			if attempt == maxRetries {
				slog.Error("max retries reached, sending batch to DLQ",
					"backend", name,
					"batch_size", len(logs),
				)
				metrics.Global.ESFlushErrors.Add(1)
				bs.sendToDLQ(logs, fmt.Sprintf("max retries exhausted: %v", err))
				return
			}

			jitter := time.Duration(rand.Intn(250)) * time.Millisecond
			delay := (baseDelay * time.Duration(1<<(attempt-1))) + jitter
			slog.Warn("retrying batch",
				"backend", name,
				"delay", delay,
				"attempt", fmt.Sprintf("%d/%d", attempt, maxRetries),
			)
			time.Sleep(delay)
			continue
		}

		// Partial failure: some items retryable, some dropped
		if dropped > 0 {
			slog.Warn("permanently dropped documents with non-retryable failures",
				"backend", name,
				"dropped", dropped,
			)
		}

		if len(retryable) == 0 {
			return
		}

		slog.Warn("retrying failed documents",
			"backend", name,
			"count", len(retryable),
		)
		logs = retryable
	}

	// If we exit the loop, all retries exhausted on partial failures
	slog.Error("max retries reached for partial failures, sending to DLQ",
		"backend", bs.flusher.Name(),
		"remaining", len(logs),
	)
	metrics.Global.ESFlushErrors.Add(1)
	bs.sendToDLQ(logs, "max retries exhausted (partial failures)")
}

func (bs *BatchSender) sendToDLQ(logs []models.LogEntry, reason string) {
	if bs.dlq == nil {
		slog.Warn("DLQ not configured, permanently dropping logs",
			"count", len(logs),
		)
		return
	}
	for _, entry := range logs {
		bs.dlq.Write(entry, reason)
	}
}
