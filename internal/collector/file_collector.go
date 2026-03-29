package collector

import (
	"bufio"
	"io"
	"os"
	"strings"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/fsnotify/fsnotify"
)

func CollectLogs(filepath string, out chan<- models.LogEntry) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Step1: Read existing CollectLogs()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		out <- models.LogEntry{
			Message: scanner.Text(),
		}
	}

	// Step2: Move cursor to end of the line
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Step3: Setup Watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	err1 := watcher.Add(filepath)
	if err1 != nil {
		return err
	}

	reader := bufio.NewReader(file)

	// Step4: Watch for new logs
	for {
		select {
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write == fsnotify.Write {
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						break
					}
					line = strings.TrimSpace(line)
					out <- models.LogEntry{
						Message: line,
					}
				}
			}
		case err := <-watcher.Errors:
			return err
		}
	}
}
