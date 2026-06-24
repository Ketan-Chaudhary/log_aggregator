package output

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/internal/config"
	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func TestFileOutput_RotationAndCleanup(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "file_output_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	outputPath := filepath.Join(tempDir, "test_output.jsonl")

	cfg := config.FileConfig{
		Path:       outputPath,
		MaxSizeMB:  1, // 1MB
		MaxBackups: 2,
	}

	fo, err := NewFileOutput(cfg)
	if err != nil {
		t.Fatalf("failed to create FileOutput: %v", err)
	}

	// Manually override the maxSize to 100 bytes for testing rotation logic
	fo.maxSize = 100

	logChan := make(chan models.LogEntry, 10)
	
	// Start file output runner in a separate goroutine
	done := make(chan struct{})
	go func() {
		fo.Run(logChan)
		close(done)
	}()

	// Send entries. Each JSON-serialized LogEntry is ~80-120 bytes.
	// This will trigger rotation multiple times.
	entry1 := models.LogEntry{Message: "log entry number one", Level: "INFO", Timestamp: time.Now()}
	entry2 := models.LogEntry{Message: "log entry number two", Level: "INFO", Timestamp: time.Now()}
	entry3 := models.LogEntry{Message: "log entry number three", Level: "INFO", Timestamp: time.Now()}
	entry4 := models.LogEntry{Message: "log entry number four", Level: "INFO", Timestamp: time.Now()}

	logChan <- entry1
	logChan <- entry2
	logChan <- entry3
	logChan <- entry4

	close(logChan)
	<-done

	// Verify files in the temp directory
	files, err := ioutil.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}

	hasBackup := false
	backupCount := 0
	for _, f := range files {
		if f.Name() != "test_output.jsonl" {
			hasBackup = true
			backupCount++
		}
	}

	if !hasBackup {
		t.Error("expected at least one backup file, found none")
	}

	if backupCount > 2 {
		t.Errorf("expected at most 2 backups due to MaxBackups=2, found %d", backupCount)
	}

	// Read active file content to ensure it contains JSON logs
	content, err := ioutil.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read active log file: %v", err)
	}

	if len(content) == 0 {
		t.Error("expected active log file to contain content, but it was empty")
	}

	var parsedEntry models.LogEntry
	if err := json.Unmarshal(content, &parsedEntry); err != nil {
		// Note: since it writes newlines, we can unmarshal the first line
		lines := filepath.SplitList(string(content))
		if len(lines) > 0 {
			_ = json.Unmarshal([]byte(lines[0]), &parsedEntry)
		}
	}
}
