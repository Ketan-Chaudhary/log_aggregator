package collector

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/metrics"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

type Manager struct {
	paths []string
	bm    *BookmarkManager
	out   chan<- models.LogEntry

	activeFiles map[string]context.CancelFunc
	mu          sync.Mutex
	wg          sync.WaitGroup
}

func NewManager(paths []string, bm *BookmarkManager, out chan<- models.LogEntry) *Manager {
	return &Manager{
		paths:       paths,
		bm:          bm,
		out:         out,
		activeFiles: make(map[string]context.CancelFunc),
	}
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	m.scanFiles(ctx)

	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			m.wg.Wait()
			return
		case <-ticker.C:
			m.scanFiles(ctx)
		}
	}
}

func (m *Manager) scanFiles(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, pattern := range m.paths {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			slog.Error("Error resolving glob pattern", "pattern", pattern, "error", err)
			continue
		}

		for _, match := range matches {
			if _, active := m.activeFiles[match]; !active {
				slog.Info("Starting collector for new file", "file", match)

				fileCtx, cancel := context.WithCancel(ctx)
				m.activeFiles[match] = cancel

				m.wg.Add(1)
				go func(path string) {
					defer m.wg.Done()
					metrics.Global.ActiveFiles.Add(1)
					err := CollectFile(fileCtx, path, m.out, m.bm)
					if err != nil && err != context.Canceled {
						slog.Error("Collector exited with error", "file", path, "error", err)
					}

					metrics.Global.ActiveFiles.Add(-1)
					m.mu.Lock()
					delete(m.activeFiles, path)
					m.mu.Unlock()
				}(match)
			}
		}
	}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cancel := range m.activeFiles {
		cancel()
	}
}
