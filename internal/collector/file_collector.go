package collector

import (
	"bufio"
	"errors"
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

	if err := scanner.Err(); err != nil {
		return err
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
	if err := watcher.Add(filepath); err != nil {
		return err
	}

	reader := bufio.NewReader(file)

	// Step4: Watch for new logs
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						if errors.Is(err, io.EOF) {
							if len(line) > 0 {
								line = strings.TrimRight(line, "\r\n")

								out <- models.LogEntry{
									Message: line,
								}
							}
							break
						}
						return err
					}
					line = strings.TrimRight(line, "\r\n")
					if line == "" {
						continue
					}
					out <- models.LogEntry{
						Message: line,
					}
				}
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}
