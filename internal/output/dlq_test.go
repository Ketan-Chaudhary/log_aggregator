package output

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
)

func TestDLQ_Write(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_dlq.jsonl")

	dlq, err := NewDLQ(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()

	entry := models.LogEntry{
		Message: "test log message",
		Source:  "/var/log/test.log",
	}

	dlq.Write(entry, "test failure reason")

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	content := string(data)
	if !strings.Contains(content, "test failure reason") {
		t.Error("DLQ file should contain the failure reason")
	}
	if !strings.Contains(content, "test log message") {
		t.Error("DLQ file should contain the log message")
	}
}

func TestDLQ_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_dlq_concurrent.jsonl")

	dlq, err := NewDLQ(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dlq.Close()

	var wg sync.WaitGroup
	numWrites := 50
	wg.Add(numWrites)

	for i := 0; i < numWrites; i++ {
		go func(id int) {
			defer wg.Done()
			entry := models.LogEntry{
				Message: "concurrent message",
				Source:  "/var/log/test.log",
			}
			dlq.Write(entry, "concurrent test")
		}(i)
	}

	wg.Wait()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != numWrites {
		t.Errorf("expected %d lines in DLQ file, got %d", numWrites, len(lines))
	}
}

func TestDLQ_Close(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_dlq_close.jsonl")

	dlq, err := NewDLQ(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := dlq.Close(); err != nil {
		t.Fatalf("expected no error on close, got %v", err)
	}
}
