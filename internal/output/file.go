package output

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

type FileOutput struct {
	path        string
	maxSize     int64 // in bytes
	maxBackups  int
	file        *os.File
	currentSize int64
	mu          sync.Mutex
}

// NewFileOutput creates and initializes a new file output plugin.
func NewFileOutput(cfg config.FileConfig) (*FileOutput, error) {
	fo := &FileOutput{
		path:       cfg.Path,
		maxSize:    cfg.MaxSizeMB * 1024 * 1024,
		maxBackups: cfg.MaxBackups,
	}

	if fo.path == "" {
		fo.path = "aggregator_output.jsonl"
	}
	if fo.maxSize <= 0 {
		fo.maxSize = 10 * 1024 * 1024 // 10 MB default
	}
	if fo.maxBackups <= 0 {
		fo.maxBackups = 5
	}

	if err := fo.openFile(); err != nil {
		return nil, err
	}

	return fo, nil
}

func (fo *FileOutput) openFile() error {
	var err error
	// Create directory if it does not exist
	dir := filepath.Dir(fo.path)
	if dir != "." && dir != "/" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory for file output: %w", err)
		}
	}

	fo.file, err = os.OpenFile(fo.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open output file: %w", err)
	}

	fi, err := fo.file.Stat()
	if err != nil {
		fo.file.Close()
		return fmt.Errorf("failed to stat output file: %w", err)
	}
	fo.currentSize = fi.Size()
	return nil
}

func (fo *FileOutput) Run(_ context.Context, in <-chan models.LogEntry) {
	defer func() {
		fo.mu.Lock()
		if fo.file != nil {
			fo.file.Close()
		}
		fo.mu.Unlock()
	}()

	for logEntry := range in {
		data, err := json.Marshal(logEntry)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error marshaling log to file: %v\n", err)
			continue
		}
		data = append(data, '\n')
		payloadSize := int64(len(data))

		fo.mu.Lock()
		if fo.currentSize+payloadSize > fo.maxSize {
			if err := fo.rotate(); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to rotate log file: %v\n", err)
				// Continue writing to active file to prevent data loss
			}
		}

		if _, err := fo.file.Write(data); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing log to file: %v\n", err)
			fo.mu.Unlock()
			continue
		}
		fo.currentSize += payloadSize
		fo.mu.Unlock()
	}
}

// rotate performs file rotation. Assumes lock is held by caller.
func (fo *FileOutput) rotate() error {
	// 1. Close active file
	if err := fo.file.Close(); err != nil {
		return fmt.Errorf("error closing active file during rotation: %w", err)
	}

	// 2. Generate backup name using timestamp: path.<timestamp>.extension
	ext := filepath.Ext(fo.path)
	base := strings.TrimSuffix(fo.path, ext)
	timestamp := time.Now().Format("20060102T150405.000") // standard sortable timestamp
	backupPath := fmt.Sprintf("%s.%s%s", base, timestamp, ext)

	// 3. Rename active file to backup path
	if err := os.Rename(fo.path, backupPath); err != nil {
		// Reopen active file before returning
		_ = fo.openFile()
		return fmt.Errorf("error renaming file during rotation: %w", err)
	}

	// 4. Open new active file
	if err := fo.openFile(); err != nil {
		return fmt.Errorf("error opening new file after rotation: %w", err)
	}

	// 5. Clean up old backups if they exceed MaxBackups limit
	fo.cleanupOldBackups()

	return nil
}

func (fo *FileOutput) cleanupOldBackups() {
	ext := filepath.Ext(fo.path)
	base := strings.TrimSuffix(fo.path, ext)
	dir := filepath.Dir(fo.path)
	pattern := filepath.Base(base) + ".*" + ext

	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return
	}

	if len(matches) <= fo.maxBackups {
		return
	}

	// Sort backups alphabetically (which is chronological due to timestamp format)
	sort.Strings(matches)

	// Delete oldest backups
	numToDelete := len(matches) - fo.maxBackups
	for i := 0; i < numToDelete; i++ {
		_ = os.Remove(matches[i])
	}
}
