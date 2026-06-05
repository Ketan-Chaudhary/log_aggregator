package collector

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/fsnotify/fsnotify"
)

func CollectLogs(ctx context.Context, filepath string, out chan<- models.LogEntry) error {
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Setup Watcher before reading to avoid race condition
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()
	if err := watcher.Add(filepath); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	var buffer string

	// Read existing content
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				buffer += line // Save partial read
				break
			}
			return err
		}
		
		fullLine := buffer + line
		buffer = ""
		fullLine = strings.TrimRight(fullLine, "\r\n")
		
		if fullLine != "" {
			select {
			case out <- models.LogEntry{Message: fullLine}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Watch for new logs
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						if errors.Is(err, io.EOF) {
							buffer += line // Buffer partial lines
							break
						}
						return err
					}
					
					fullLine := buffer + line
					buffer = ""
					fullLine = strings.TrimRight(fullLine, "\r\n")
					
					if fullLine == "" {
						continue
					}
					
					select {
					case out <- models.LogEntry{Message: fullLine}:
					case <-ctx.Done():
						return ctx.Err()
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
