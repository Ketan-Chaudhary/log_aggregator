package collector

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Ketan-Chaudhary/log_aggregator/pkg/models"
	"github.com/fsnotify/fsnotify"
)

func CollectLogs(ctx context.Context, filepath string, out chan<- models.LogEntry) error {
	for {
		err := watchFile(ctx, filepath, out)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
			}
		} else {
			// wait a little before re-opening to avoid CPU spinning if rotation is fast
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func watchFile(ctx context.Context, filepath string, out chan<- models.LogEntry) error {
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(1 * time.Second):
				return nil
			}
		}
		return err
	}
	defer file.Close()

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

	readLines := func() error {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if errors.Is(err, io.EOF) {
					buffer += line
					break
				}
				return err
			}
			
			fullLine := buffer + line
			buffer = ""
			fullLine = strings.TrimRight(fullLine, "\r\n")
			
			if fullLine != "" {
				select {
				case out <- models.LogEntry{Message: fullLine, Source: filepath}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	}

	if err := readLines(); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				if err := readLines(); err != nil {
					return err
				}
			}
			if event.Op&fsnotify.Rename == fsnotify.Rename || event.Op&fsnotify.Remove == fsnotify.Remove {
				// file was rotated, close and reopen
				return nil
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			return err
		}
	}
}
